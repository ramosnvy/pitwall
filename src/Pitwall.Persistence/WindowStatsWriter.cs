using System.Threading.Channels;
using Npgsql;
using NpgsqlTypes;
using Pitwall.Processing.Core;

namespace Pitwall.Persistence;

/// <summary>
/// Grava as janelas fechadas no PostgreSQL usando COPY binario do Npgsql.
///
/// Escrita em lote e por uma unica thread dedicada, fora do caminho de
/// processamento: se a persistencia acontecesse na thread que processa, o
/// tempo do banco entraria na latencia medida e a comparacao entre
/// arquiteturas passaria a medir o PostgreSQL.
///
/// COPY binario em vez de INSERT porque, no nivel de carga alto, sao dezenas
/// de milhares de janelas por segundo -- uma instrucao por linha faria o
/// banco virar o gargalo e achataria as diferencas entre as variantes
/// (ver docs/AMEACAS-VALIDADE.md).
/// </summary>
public sealed class WindowStatsWriter : IAsyncDisposable
{
    private readonly Channel<DriverWindowStats> _queue;
    private readonly Task _writer;
    private readonly string _connectionString;
    private readonly Guid _runId;
    private readonly string _architecture;
    private readonly int _batchSize;

    private long _written;
    private long _dropped;

    public WindowStatsWriter(
        string connectionString,
        Guid runId,
        string architecture,
        int batchSize = 10_000,
        int queueCapacity = 200_000)
    {
        _connectionString = connectionString;
        _runId = runId;
        _architecture = architecture;
        _batchSize = batchSize;

        // Fila limitada com descarte do mais antigo: se o banco nao acompanhar,
        // e preferivel perder linha de saida a aplicar contrapressao sobre o
        // processamento, o que contaminaria a latencia medida. O numero de
        // descartes e reportado -- rodada com descarte nao serve para analise
        // de completude, so para latencia e vazao.
        _queue = Channel.CreateBounded<DriverWindowStats>(
            new BoundedChannelOptions(queueCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            },
            _ => Interlocked.Increment(ref _dropped));

        _writer = Task.Run(WriteLoopAsync);
    }

    public long Written => Interlocked.Read(ref _written);
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Enfileira uma janela. Chamado pelos workers de processamento.</summary>
    public void Enqueue(in DriverWindowStats stats) => _queue.Writer.TryWrite(stats);

    private async Task WriteLoopAsync()
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var batch = new List<DriverWindowStats>(_batchSize);

        await foreach (var stats in _queue.Reader.ReadAllAsync())
        {
            batch.Add(stats);

            if (batch.Count >= _batchSize)
            {
                await FlushAsync(connection, batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await FlushAsync(connection, batch);
        }
    }

    private async Task FlushAsync(NpgsqlConnection connection, List<DriverWindowStats> batch)
    {
        await using var importer = await connection.BeginBinaryImportAsync(
            "COPY driver_window_stats (run_id, architecture, driver_number, window_start, " +
            "window_end, event_count, avg_speed, max_speed, hard_brakings, gear_changes) " +
            "FROM STDIN (FORMAT BINARY)");

        foreach (var stats in batch)
        {
            await importer.StartRowAsync();
            await importer.WriteAsync(_runId, NpgsqlDbType.Uuid);
            await importer.WriteAsync(_architecture, NpgsqlDbType.Text);
            await importer.WriteAsync(stats.DriverNumber, NpgsqlDbType.Integer);
            await importer.WriteAsync(stats.WindowStart, NpgsqlDbType.TimestampTz);
            await importer.WriteAsync(stats.WindowEnd, NpgsqlDbType.TimestampTz);
            await importer.WriteAsync(stats.EventCount, NpgsqlDbType.Integer);
            await importer.WriteAsync(stats.AvgSpeed, NpgsqlDbType.Real);
            await importer.WriteAsync(stats.MaxSpeed, NpgsqlDbType.Smallint);
            await importer.WriteAsync(stats.HardBrakings, NpgsqlDbType.Integer);
            await importer.WriteAsync(stats.GearChanges, NpgsqlDbType.Integer);
        }

        await importer.CompleteAsync();
        Interlocked.Add(ref _written, batch.Count);
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        await _writer;
    }
}
