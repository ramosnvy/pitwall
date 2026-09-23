using System.Diagnostics;
using Pitwall.Contracts;

namespace Pitwall.Processing.Core;

/// <summary>
/// Agregacao em janela por carro, com deteccao de padrao sobre eventos
/// consecutivos. E a mesma logica nas seis variantes da matriz -- o que muda
/// entre elas e o transporte, nunca o trabalho realizado.
///
/// **Esta classe nao e thread-safe, de proposito.** Cada worker processa um
/// subconjunto disjunto de carros (particionado por numero do carro), o que
/// garante duas coisas: as amostras de um carro sao vistas em ordem, e o
/// resultado nao depende de quantos workers existem. Sem isso, duas threads
/// processando o mesmo carro contariam frenagens de forma nao-deterministica
/// e a verificacao cruzada entre as variantes perderia o sentido.
/// </summary>
public sealed class TelemetryProcessor(ProcessingOptions options, Action<DriverWindowStats> onWindowClosed)
{
    private readonly Dictionary<int, CarState> _cars = new(capacity: 1024);
    private readonly long _windowTicks = options.WindowSize.Ticks;
    private readonly long _syntheticCostTicks =
        (long)(options.SyntheticCostMicros * Stopwatch.Frequency / 1_000_000);

    public long EventsProcessed { get; private set; }
    public long WindowsClosed { get; private set; }

    public void Process(in TelemetryEvent evt)
    {
        ref var car = ref GetState(evt.DriverNumber, evt.EventTime, out var isNew);

        var windowStart = FloorToWindow(evt.EventTime);

        // A janela do carro virou: fecha a anterior e comeca outra. Janelas
        // sem evento nenhum simplesmente nao existem, o que e o comportamento
        // correto para um carro que saiu da corrida.
        if (!isNew && windowStart > car.WindowStart)
        {
            Emit(evt.DriverNumber, in car);
            car.ResetWindow(windowStart);
        }

        // Deteccao de padrao: depende do evento anterior do mesmo carro, e e
        // por isso que a ordem por carro precisa ser preservada do broker ate
        // aqui. Se a ordem quebrar, estas contagens saem erradas -- o que faz
        // delas um detector de defeito no pipeline.
        if (!isNew)
        {
            if (evt.Brake >= options.BrakeThreshold && car.LastBrake < options.BrakeThreshold)
            {
                car.HardBrakings++;
            }

            if (evt.Gear != car.LastGear && evt.Gear > 0)
            {
                car.GearChanges++;
            }
        }

        car.EventCount++;
        car.SpeedSum += evt.Speed;

        if (evt.Speed > car.MaxSpeed)
        {
            car.MaxSpeed = evt.Speed;
        }

        car.LastBrake = evt.Brake;
        car.LastGear = evt.Gear;

        EventsProcessed++;

        if (_syntheticCostTicks > 0)
        {
            BurnCpu(_syntheticCostTicks);
        }
    }

    /// <summary>
    /// Fecha as janelas ainda abertas. Chamado no fim da rodada: sem isso, a
    /// ultima janela de cada carro ficaria de fora e as variantes divergiriam
    /// conforme o momento em que cada uma parou.
    /// </summary>
    public void Complete()
    {
        foreach (var (driver, state) in _cars)
        {
            if (state.EventCount > 0)
            {
                Emit(driver, state);
            }
        }

        _cars.Clear();
    }

    private ref CarState GetState(int driverNumber, DateTimeOffset eventTime, out bool isNew)
    {
        ref var car = ref System.Runtime.InteropServices.CollectionsMarshal
            .GetValueRefOrAddDefault(_cars, driverNumber, out var existed);

        isNew = !existed;

        if (isNew)
        {
            car = new CarState();
            car.ResetWindow(FloorToWindow(eventTime));
        }

        return ref car!;
    }

    private void Emit(int driverNumber, in CarState car)
    {
        onWindowClosed(new DriverWindowStats
        {
            DriverNumber = driverNumber,
            WindowStart = car.WindowStart,
            WindowEnd = car.WindowStart.AddTicks(_windowTicks),
            EventCount = car.EventCount,
            AvgSpeed = car.EventCount > 0 ? (float)car.SpeedSum / car.EventCount : 0,
            MaxSpeed = car.MaxSpeed,
            HardBrakings = car.HardBrakings,
            GearChanges = car.GearChanges
        });

        WindowsClosed++;
    }

    private DateTimeOffset FloorToWindow(DateTimeOffset value) =>
        new(value.UtcTicks - value.UtcTicks % _windowTicks, TimeSpan.Zero);

    /// <summary>
    /// Consome tempo de CPU de forma previsivel. Espera ativa em vez de
    /// Sleep: o objetivo e ocupar o processador, nao liberar a thread.
    /// </summary>
    private static void BurnCpu(long ticks)
    {
        var deadline = Stopwatch.GetTimestamp() + ticks;
        while (Stopwatch.GetTimestamp() < deadline)
        {
            Thread.SpinWait(1);
        }
    }

    /// <summary>Estado da janela corrente de um carro.</summary>
    private struct CarState
    {
        public DateTimeOffset WindowStart;
        public int EventCount;
        public long SpeedSum;
        public short MaxSpeed;
        public int HardBrakings;
        public int GearChanges;
        public short LastBrake;
        public short LastGear;

        public void ResetWindow(DateTimeOffset start)
        {
            WindowStart = start;
            EventCount = 0;
            SpeedSum = 0;
            MaxSpeed = 0;
            HardBrakings = 0;
            GearChanges = 0;
        }
    }
}
