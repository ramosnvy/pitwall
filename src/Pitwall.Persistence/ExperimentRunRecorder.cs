using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Pitwall.Persistence;

/// <summary>
/// Registra uma linha por execucao na tabela <c>experiment_run</c>.
///
/// E o que torna cada numero do artigo rastreavel ate a rodada que o gerou:
/// qual arquitetura, sob que carga, com que parametros e em que momento. Sem
/// esse registro, um CSV de resultados sem procedencia nao e reproduzivel.
/// </summary>
public sealed class ExperimentRunRecorder(string connectionString)
{
    public async Task<Guid> StartAsync(
        Guid runId,
        string architecture,
        string broker,
        string mechanism,
        int targetRate,
        int replication,
        bool persistenceOn,
        IReadOnlyDictionary<string, object?> config,
        CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await using var command = new NpgsqlCommand(
            """
            INSERT INTO experiment_run
                (run_id, architecture, broker, mechanism, target_rate, replication,
                 persistence_on, started_at, config)
            VALUES (@run_id, @architecture, @broker, @mechanism, @target_rate, @replication,
                    @persistence_on, now(), @config)
            """,
            connection);

        command.Parameters.AddWithValue("run_id", runId);
        command.Parameters.AddWithValue("architecture", architecture);
        command.Parameters.AddWithValue("broker", broker);
        command.Parameters.AddWithValue("mechanism", mechanism);
        command.Parameters.AddWithValue("target_rate", targetRate);
        command.Parameters.AddWithValue("replication", (short)replication);
        command.Parameters.AddWithValue("persistence_on", persistenceOn);
        command.Parameters.Add(new NpgsqlParameter("config", NpgsqlDbType.Jsonb)
        {
            Value = JsonSerializer.Serialize(config)
        });

        await command.ExecuteNonQueryAsync(ct);
        return runId;
    }

    public async Task FinishAsync(Guid runId, string? notes = null, CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await using var command = new NpgsqlCommand(
            "UPDATE experiment_run SET finished_at = now(), notes = @notes WHERE run_id = @run_id",
            connection);

        command.Parameters.AddWithValue("run_id", runId);
        command.Parameters.AddWithValue("notes", (object?)notes ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Esvazia as tabelas de saida entre rodadas. Sem isso, o acumulo de
    /// dezenas de milhoes de linhas faria cada rodada encontrar um banco
    /// diferente da anterior, e a comparacao mediria o tamanho da tabela.
    /// </summary>
    public async Task TruncateOutputsAsync(CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await using var command = new NpgsqlCommand(
            "TRUNCATE processed_event, driver_window_stats", connection);

        await command.ExecuteNonQueryAsync(ct);
    }
}
