<#
.SYNOPSIS
Fracao de periodos CFS em que cada container foi estrangulado (throttled) em
cada rodada da matriz, a partir do historico do Prometheus. Ver docs/IMPLEMENTACAO.md.
#>
param([string]$In = "results/matrix-v2-runs.csv", [string]$Prometheus = "http://localhost:9090")
$root = Split-Path $PSScriptRoot -Parent
$inv = [cultureinfo]::InvariantCulture
$P = "$Prometheus/api/v1/query"

function Q([string]$expr, [long]$time) {
    $url = $P + '?query=' + [uri]::EscapeDataString($expr) + '&time=' + $time
    $res = (Invoke-RestMethod $url).data.result
    if (-not $res) { return 0.0 }
    return [double](($res | ForEach-Object { [double]::Parse($_.value[1], $inv) } | Measure-Object -Sum).Sum)
}

function Pct([string]$name, [long]$time) {
    $t = Q ('increase(container_cpu_cfs_throttled_periods_total{name="' + $name + '"}[100s])') $time
    $p = Q ('increase(container_cpu_cfs_periods_total{name="' + $name + '"}[100s])') $time
    if ($p -le 0) { return [double]::NaN }
    return 100 * $t / $p
}

$rows = Import-Csv (Join-Path $root $In)
$out = foreach ($r in $rows) {
    $end = [DateTimeOffset]::Parse($r.timestamp, $inv).ToUnixTimeSeconds()
    $broker = if ($r.architecture -like 'kafka*') { 'pitwall-kafka' } else { 'pitwall-rabbitmq' }
    [pscustomobject]@{
        broker   = ($r.architecture -split '-')[0]
        carga    = [int]$r.target_rate
        thr_b    = Pct $broker $end
        thr_c    = Pct 'pitwall-consumer' $end
        thr_p    = Pct 'pitwall-producer' $end
        p99      = [double]::Parse($r.p99_us, $inv) / 1000
    }
}

function Avg($xs) { $v = @($xs | Where-Object { -not [double]::IsNaN($_) }); if ($v.Count) { [math]::Round(($v | Measure-Object -Average).Average, 2) } else { 'n/d' } }

$out | Group-Object broker, carga | ForEach-Object {
    $g = $_.Group
    $sorted = @($g.p99 | Sort-Object)
    [pscustomobject]@{
        broker = $g[0].broker; carga = $g[0].carga; n = $g.Count
        throttle_broker_pct = Avg $g.thr_b
        throttle_consumidor_pct = Avg $g.thr_c
        throttle_produtor_pct = Avg $g.thr_p
        p99_mediana_ms = [math]::Round($sorted[[math]::Floor($sorted.Count / 2)], 2)
    }
} | Sort-Object broker, carga | Format-Table -AutoSize
