using System.Diagnostics;

namespace Pitwall.Consumer.Republish;

/// <summary>
/// At-least-once de ponta a ponta no segundo cenario (fase 9A,
/// docs/APROFUNDAMENTO.md secao 8.6): a mensagem de entrada so e confirmada
/// ao broker de entrada depois que a saida que depende dela foi confirmada
/// pelo broker de saida.
///
/// Por faixa, guarda as pendencias em ordem de posicao de entrada (offset do
/// Kafka ou etiqueta de entrega do RabbitMQ, ambos crescentes por faixa). Uma
/// pendencia nasce aberta (a janela ainda nao fechou, ou a saida ainda nao
/// saiu), passa a publicada e termina confirmada. A posicao liberada e a
/// anterior a pendencia mais antiga ainda nao confirmada: como as
/// confirmacoes chegam fora de ordem entre janelas de carros diferentes, so se
/// libera o trecho continuo -- a tecnica do Confluent Parallel Consumer.
///
/// As confirmacoes chegam de outra thread (relatorio de entrega do Kafka,
/// confirmacao do RabbitMQ); por isso cada faixa tem a sua trava.
/// </summary>
public sealed class ReleaseTracker
{
    private readonly LaneState[] _lanes;
    private long _waitTicks;
    private long _waits;

    public ReleaseTracker(int lanes)
    {
        _lanes = Enumerable.Range(0, lanes).Select(_ => new LaneState()).ToArray();
    }

    /// <summary>Tempo total parado esperando confirmacao da saida, somado entre as faixas.</summary>
    public TimeSpan WaitTime => TimeSpan.FromSeconds(Interlocked.Read(ref _waitTicks) / (double)Stopwatch.Frequency);

    /// <summary>Quantas vezes alguma faixa precisou esperar confirmacao.</summary>
    public long Waits => Interlocked.Read(ref _waits);

    /// <summary>Abre uma pendencia que comeca na posicao de entrada indicada. Devolve o identificador dela.</summary>
    public long Open(int lane, long position) => _lanes[lane].Open(position);

    /// <summary>A saida da pendencia foi entregue ao broker e espera confirmacao.</summary>
    public void MarkPublished(int lane, long id) => _lanes[lane].MarkPublished(id);

    /// <summary>O broker de saida confirmou a saida da pendencia.</summary>
    public void Confirm(int lane, long id) => _lanes[lane].Confirm(id);

    /// <summary>A entrada nesta posicao ja foi processada.</summary>
    public void Processed(int lane, long position) => _lanes[lane].Processed(position);

    /// <summary>Maior posicao de entrada que ja pode ser confirmada; -1 se nenhuma.</summary>
    public long Released(int lane) => _lanes[lane].Released();

    /// <summary>Saidas publicadas e ainda nao confirmadas na faixa.</summary>
    public int InFlight(int lane) => _lanes[lane].InFlight;

    /// <summary>Pendencias da faixa ainda nao confirmadas, abertas ou publicadas.</summary>
    public long Pending(int lane) => _lanes[lane].Pending;

    /// <summary>
    /// Completa quando a faixa tem no maximo <paramref name="limit"/> saidas em
    /// voo. Com 0, espera todas as confirmacoes: e a espera do modo Direct.
    /// </summary>
    public ValueTask WaitInFlightAtMostAsync(int lane, int limit)
    {
        var wait = _lanes[lane].WaitAtMost(limit);

        if (wait is null)
        {
            return ValueTask.CompletedTask;
        }

        return Measure(wait);
    }

    private async ValueTask Measure(Task wait)
    {
        var start = Stopwatch.GetTimestamp();
        await wait.ConfigureAwait(false);
        Interlocked.Add(ref _waitTicks, Stopwatch.GetTimestamp() - start);
        Interlocked.Increment(ref _waits);
    }

    private sealed class LaneState
    {
        private const byte Aberta = 0;
        private const byte Publicada = 1;
        private const byte Confirmada = 2;

        private readonly Lock _gate = new();
        private long[] _positions = new long[1024];
        private byte[] _states = new byte[1024];
        private long _head;
        private long _tail;
        private long _lastProcessed = -1;
        private int _inFlight;
        private TaskCompletionSource? _waiter;
        private int _waitLimit;

        public int InFlight
        {
            get { lock (_gate) return _inFlight; }
        }

        public long Pending
        {
            get { lock (_gate) return _tail - _head; }
        }

        public long Open(long position)
        {
            lock (_gate)
            {
                if (_tail - _head == _positions.Length)
                {
                    Grow();
                }

                var id = _tail++;
                var slot = (int)(id % _positions.Length);
                _positions[slot] = position;
                _states[slot] = Aberta;
                return id;
            }
        }

        public void MarkPublished(long id)
        {
            lock (_gate)
            {
                _states[(int)(id % _states.Length)] = Publicada;
                _inFlight++;
            }
        }

        public void Confirm(long id)
        {
            TaskCompletionSource? release = null;

            lock (_gate)
            {
                _states[(int)(id % _states.Length)] = Confirmada;
                _inFlight--;

                while (_head < _tail && _states[(int)(_head % _states.Length)] == Confirmada)
                {
                    _head++;
                }

                if (_waiter is not null && _inFlight <= _waitLimit)
                {
                    release = _waiter;
                    _waiter = null;
                }
            }

            release?.TrySetResult();
        }

        public void Processed(long position)
        {
            lock (_gate)
            {
                if (position > _lastProcessed)
                {
                    _lastProcessed = position;
                }
            }
        }

        public long Released()
        {
            lock (_gate)
            {
                if (_head == _tail)
                {
                    return _lastProcessed;
                }

                return Math.Min(_lastProcessed, _positions[(int)(_head % _positions.Length)] - 1);
            }
        }

        public Task? WaitAtMost(int limit)
        {
            lock (_gate)
            {
                if (_inFlight <= limit)
                {
                    return null;
                }

                // Um so esperador por faixa: quem espera e sempre a etapa que
                // publica naquela faixa.
                _waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waitLimit = limit;
                return _waiter.Task;
            }
        }

        private void Grow()
        {
            var length = _positions.Length * 2;
            var positions = new long[length];
            var states = new byte[length];

            for (var id = _head; id < _tail; id++)
            {
                positions[(int)(id % length)] = _positions[(int)(id % _positions.Length)];
                states[(int)(id % length)] = _states[(int)(id % _states.Length)];
            }

            _positions = positions;
            _states = states;
        }
    }
}
