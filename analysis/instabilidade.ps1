<#
.SYNOPSIS
Cruza as linhas do tempo da fase 4 (produtor, consumidor e GC do broker) para
achar a origem dos picos de latencia e de atraso de emissao do Kafka.

.DESCRIPTION
Para cada rodada de -In, le results/traces/<run_id>-{produtor,consumidor}.csv
e <run_id>-broker-gc.log. Um episodio e uma sequencia de intervalos de 100 ms
em que a latencia maxima do consumidor ou o atraso de emissao do produtor
passa do limite (50 ms). Para cada episodio, olha o segundo anterior e o
proprio episodio:

  confirmacoes   quantas mensagens o broker confirmou ao produtor, contra
                 quantas foram emitidas; confirmacao parada com emissao
                 normal e o broker sem responder;
  gc_*           pausas de coleta de lixo no produtor, no consumidor e no
                 broker (G1);
  pool           maior fila de trabalho pendente no pool de threads do
                 consumidor;
  cpu            CPU por intervalo no produtor e no consumidor.

Escreve <prefixo>-episodios.csv (um por episodio) e <prefixo>-rodadas.csv
(um por rodada, com o maior episodio).

.EXAMPLE
./instabilidade.ps1 -In results/fase4-runs.csv -Out results/fase4
#>
param(
    [string]$In = 'results/fase4-runs.csv',
    [string]$Out = 'results/fase4',
    [double]$LimitMs = 50,
    [double]$LookbackMs = 1000
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture
$traces = Join-Path $root 'results/traces'

function Num($v) {
    if ([string]::IsNullOrWhiteSpace($v)) { return 0.0 }
    return [double]::Parse($v, $inv)
}

function Utc([string]$text) {
    return [datetime]::Parse($text, $inv, [System.Globalization.DateTimeStyles]::AdjustToUniversal -bor [System.Globalization.DateTimeStyles]::AssumeUniversal)
}

function ReadTrace([string]$path) {
    if (-not (Test-Path $path)) { return @() }
    return @(Import-Csv $path | ForEach-Object {
        $_ | Add-Member -NotePropertyName t -NotePropertyValue (Utc $_.utc) -PassThru
    })
}

function ReadBrokerGc([string]$path) {
    if (-not (Test-Path $path)) { return @() }
    return @(Get-Content $path | ForEach-Object {
        if ($_ -match '^\[(\S+)\+0000\].*\s([0-9.]+)ms$') {
            [pscustomobject]@{ t = (Utc ($Matches[1] + 'Z')); ms = [double]::Parse($Matches[2], $inv) }
        }
    })
}

function InWindow($rows, [datetime]$from, [datetime]$to) {
    return @($rows | Where-Object { $_.t -ge $from -and $_.t -le $to })
}

function SumOf($rows, [string]$field) {
    $s = 0.0
    foreach ($r in $rows) { $s += Num $r.$field }
    return $s
}

function MaxOf($rows, [string]$field) {
    $m = 0.0
    foreach ($r in $rows) { $m = [math]::Max($m, (Num $r.$field)) }
    return $m
}

$limitUs = $LimitMs * 1000
$episodes = @()
$perRun = @()

foreach ($run in (Import-Csv (Join-Path $root $In))) {
    $id = $run.run_id
    $producer = ReadTrace (Join-Path $traces "$id-produtor.csv")
    $consumer = ReadTrace (Join-Path $traces "$id-consumidor.csv")
    $brokerGc = ReadBrokerGc (Join-Path $traces "$id-broker-gc.log")

    # Instantes acima do limite, de qualquer lado, agrupados em episodios
    # quando distam ate 200 ms um do outro.
    $marks = @(
        $producer | Where-Object { (Num $_.lateness_max_us) -gt $limitUs } | ForEach-Object { $_.t }
        $consumer | Where-Object { (Num $_.latency_max_us) -gt $limitUs } | ForEach-Object { $_.t }
    ) | Sort-Object

    $groups = @()
    foreach ($m in $marks) {
        if ($groups.Count -gt 0 -and ($m - $groups[-1].to).TotalMilliseconds -le 200) { $groups[-1].to = $m }
        else { $groups += [pscustomobject]@{ from = $m; to = $m } }
    }

    $runEpisodes = foreach ($g in $groups) {
        $from = $g.from.AddMilliseconds(-$LookbackMs)
        $to = $g.to
        $p = InWindow $producer $from $to
        $c = InWindow $consumer $from $to
        $b = InWindow $brokerGc $from $to

        $emitted = SumOf $p 'emitted'
        $confirmed = SumOf $p 'confirmed'
        $zeroConfirm = @($p | Where-Object { (Num $_.confirmed) -eq 0 -and (Num $_.emitted) -gt 0 }).Count

        [pscustomobject]@{
            run_id = $id; lado = $run.lado; carga = $run.target_rate
            inicio_utc = $g.from.ToString('HH:mm:ss.fff', $inv)
            duracao_ms = [int]($g.to - $g.from).TotalMilliseconds + 100
            pico_latencia_ms = [math]::Round((MaxOf (InWindow $consumer $g.from $g.to) 'latency_max_us') / 1000, 1)
            pico_atraso_ms = [math]::Round((MaxOf (InWindow $producer $g.from $g.to) 'lateness_max_us') / 1000, 1)
            confirmado_sobre_emitido = if ($emitted -gt 0) { [math]::Round($confirmed / $emitted, 2) } else { '' }
            intervalos_sem_confirmar = $zeroConfirm
            fila_cheia = SumOf $p 'queue_full'
            gc_produtor_ms = [math]::Round((SumOf $p 'gc_pause_ms'), 1)
            gc_consumidor_ms = [math]::Round((SumOf $c 'gc_pause_ms'), 1)
            gc_broker_ms = [math]::Round((($b | Measure-Object ms -Sum).Sum), 1)
            gc_broker_maior_ms = [math]::Round((($b | Measure-Object ms -Maximum).Maximum), 1)
            gc2_consumidor = SumOf $c 'gc2'
            pool_pendente_max = MaxOf $c 'pool_pending'
            cpu_produtor_ms = if ($p.Count) { [math]::Round((SumOf $p 'cpu_ms') / $p.Count, 0) } else { '' }
            cpu_consumidor_ms = if ($c.Count) { [math]::Round((SumOf $c 'cpu_ms') / $c.Count, 0) } else { '' }
        }
    }

    $episodes += @($runEpisodes)
    $worst = @($runEpisodes) | Sort-Object { [math]::Max($_.pico_latencia_ms, $_.pico_atraso_ms) } -Descending | Select-Object -First 1

    $perRun += [pscustomobject]@{
        run_id = $id; lado = $run.lado; carga = $run.target_rate
        p99_ms = [math]::Round((Num $run.p99_us) / 1000, 2)
        atraso_ms = $run.producer_jitter_ms
        episodios = @($runEpisodes).Count
        tem_linha_do_tempo = ($producer.Count -gt 0 -and $consumer.Count -gt 0)
        pior_inicio_utc = $worst.inicio_utc
        pior_latencia_ms = $worst.pico_latencia_ms
        pior_atraso_ms = $worst.pico_atraso_ms
        pior_confirmado_sobre_emitido = $worst.confirmado_sobre_emitido
        pior_gc_produtor_ms = $worst.gc_produtor_ms
        pior_gc_consumidor_ms = $worst.gc_consumidor_ms
        pior_gc_broker_ms = $worst.gc_broker_maior_ms
    }
}

$prefix = Join-Path $root $Out
$episodes | Export-Csv "$prefix-episodios.csv" -NoTypeInformation -Encoding UTF8
$perRun | Export-Csv "$prefix-rodadas.csv" -NoTypeInformation -Encoding UTF8
Write-Host "$($perRun.Count) rodadas, $($episodes.Count) episodios acima de $LimitMs ms."
Write-Host "Saida: $prefix-episodios.csv e $prefix-rodadas.csv"
