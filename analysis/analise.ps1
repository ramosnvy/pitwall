<#
.SYNOPSIS
Analise pre-registrada da matriz (docs/ANALISE.md): validade, descarte de
Tukey, resumo por combinacao, ponto de saturacao e comparacoes.

.DESCRIPTION
Le um ou mais CSVs consolidados de rodadas (o -runs.csv do run-matrix, do
sweep ou do decisoes-fase2) e escreve quatro CSVs com o mesmo prefixo:

  <prefixo>-rodadas.csv    cada rodada, valida ou nao, com o motivo e o
                           descarte de Tukey;
  <prefixo>-resumo.csv     uma linha por combinacao (broker x modo x carga,
                           dentro do perfil e da frequencia);
  <prefixo>-saturacao.csv  ponto de saturacao por broker x modo, e cada carga
                           com a condicao que falhou;
  <prefixo>-modos.csv      efeito do mecanismo interno contra o Direct e o
                           teste de Kruskal-Wallis entre os tres modos.

Mudar uma regra aqui exige registrar a mudanca em docs/ANALISE.md.

.EXAMPLE
./analise.ps1 -In results/matrix-v3-runs.csv -Out results/analise-v3
#>
param(
    [string[]]$In = @('results/matrix-v3-runs.csv'),
    [string]$Out = 'results/analise-v3',
    [double]$JitterCeilingMs = 50,
    [double]$SaturationThroughput = 0.99,
    [double]$SaturationP99Ms = 50,
    [double]$RelevantThroughput = 0.10,
    [double]$RelevantP99 = 0.20,
    [int]$MinRunsForTukey = 5
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Num($value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return [double]::NaN }
    $n = 0.0
    if ([double]::TryParse(($value -replace ',', '.'), [System.Globalization.NumberStyles]::Float, $inv, [ref]$n)) { return $n }
    return [double]::NaN
}

function Median([double[]]$values) {
    $v = @($values | Where-Object { -not [double]::IsNaN($_) } | Sort-Object)
    if ($v.Count -eq 0) { return [double]::NaN }
    $mid = [math]::Floor($v.Count / 2)
    if ($v.Count % 2 -eq 1) { return [double]$v[$mid] }
    return ([double]$v[$mid - 1] + [double]$v[$mid]) / 2
}

function StdDev([double[]]$values) {
    $v = @($values | Where-Object { -not [double]::IsNaN($_) })
    if ($v.Count -lt 2) { return [double]::NaN }
    $mean = ($v | Measure-Object -Average).Average
    $sum = 0.0
    foreach ($x in $v) { $sum += ($x - $mean) * ($x - $mean) }
    return [math]::Sqrt($sum / ($v.Count - 1))
}

# Quantil por interpolacao linear (tipo 7 do R, o do Excel).
function Quantile([double[]]$sorted, [double]$p) {
    $h = ($sorted.Count - 1) * $p
    $lo = [math]::Floor($h)
    $hi = [math]::Ceiling($h)
    return $sorted[$lo] + ($h - $lo) * ($sorted[$hi] - $sorted[$lo])
}

# Gama incompleta regularizada superior Q(a, x), para o p-valor do
# qui-quadrado (Numerical Recipes: serie para x < a + 1, fracao continua acima).
function LogGamma([double]$z) {
    $c = @(76.18009172947146, -86.50532032941677, 24.01409824083091,
           -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5)
    $x = $z; $y = $z
    $tmp = $x + 5.5
    $tmp -= ($x + 0.5) * [math]::Log($tmp)
    $ser = 1.000000000190015
    foreach ($ci in $c) { $y += 1; $ser += $ci / $y }
    return -$tmp + [math]::Log(2.5066282746310005 * $ser / $x)
}

function GammaQ([double]$a, [double]$x) {
    if ($x -le 0) { return 1.0 }
    $gln = LogGamma $a
    if ($x -lt $a + 1) {
        $ap = $a; $sum = 1.0 / $a; $del = $sum
        for ($n = 0; $n -lt 500; $n++) {
            $ap += 1; $del *= $x / $ap; $sum += $del
            if ([math]::Abs($del) -lt [math]::Abs($sum) * 1e-12) { break }
        }
        return 1.0 - $sum * [math]::Exp(-$x + $a * [math]::Log($x) - $gln)
    }
    $b = $x + 1 - $a; $c = 1e300; $d = 1 / $b; $h = $d
    for ($i = 1; $i -le 500; $i++) {
        $an = -$i * ($i - $a); $b += 2
        $d = $an * $d + $b; if ([math]::Abs($d) -lt 1e-300) { $d = 1e-300 }
        $c = $b + $an / $c; if ([math]::Abs($c) -lt 1e-300) { $c = 1e-300 }
        $d = 1 / $d; $del = $d * $c; $h *= $del
        if ([math]::Abs($del - 1) -lt 1e-12) { break }
    }
    return [math]::Exp(-$x + $a * [math]::Log($x) - $gln) * $h
}

# Kruskal-Wallis com correcao de empates. Devolve H e o p-valor.
function KruskalWallis([hashtable]$groups) {
    $all = @()
    foreach ($k in $groups.Keys) { foreach ($v in $groups[$k]) { $all += [pscustomobject]@{ g = $k; v = $v } } }
    $n = $all.Count
    $k = $groups.Keys.Count
    if ($k -lt 2 -or $n -le $k) { return @{ H = [double]::NaN; P = [double]::NaN } }

    $sorted = @($all | Sort-Object v)
    $ranks = @{}
    foreach ($key in $groups.Keys) { $ranks[$key] = 0.0 }
    $ties = 0.0
    $i = 0
    while ($i -lt $n) {
        $j = $i
        while ($j + 1 -lt $n -and $sorted[$j + 1].v -eq $sorted[$i].v) { $j++ }
        $rank = ($i + $j) / 2.0 + 1
        for ($m = $i; $m -le $j; $m++) { $ranks[$sorted[$m].g] += $rank }
        $t = $j - $i + 1
        $ties += $t * $t * $t - $t
        $i = $j + 1
    }

    $h = 0.0
    foreach ($key in $groups.Keys) { $h += $ranks[$key] * $ranks[$key] / $groups[$key].Count }
    $h = 12.0 / ($n * ($n + 1)) * $h - 3 * ($n + 1)
    $correction = 1 - $ties / ($n * $n * $n - $n)
    if ($correction -gt 0) { $h /= $correction }

    return @{ H = $h; P = (GammaQ (($k - 1) / 2.0) ($h / 2.0)) }
}

function Fmt([double]$v, [string]$format = '0.###') {
    if ([double]::IsNaN($v)) { return '' }
    return $v.ToString($format, $inv)
}

# --- leitura ---------------------------------------------------------------

$rows = foreach ($path in $In) {
    foreach ($r in (Import-Csv (Join-Path $root $path))) {
        $arch = $r.architecture
        $broker = ($arch -split '-')[0]
        [pscustomobject]@{
            source         = Split-Path $path -Leaf
            run_id         = $r.run_id
            broker         = $broker
            mode           = $arch.Substring($broker.Length + 1)
            rate           = [int](Num $r.target_rate)
            profile        = if ($r.profile) { $r.profile } else { 'anterior' }
            hz             = if ($r.sample_hz) { $r.sample_hz } else { 'nativo' }
            window         = $r.window_start_s
            seconds        = $r.measure_seconds
            decisao        = $r.decisao
            lado           = $r.lado
            throughput     = Num $r.throughput
            p50            = (Num $r.p50_us) / 1000
            p95            = (Num $r.p95_us) / 1000
            p99            = (Num $r.p99_us) / 1000
            max            = (Num $r.max_us) / 1000
            jitter         = Num $r.producer_jitter_ms
            digest         = $r.digest_hash
            incomplete     = Num $r.incomplete_records
            dropped        = Num $r.windows_dropped
            cpu_consumer   = Num $r.consumer_container_cpu_avg
            cpu_broker     = Num $r.broker_cpu_avg
            cpu_producer   = Num $r.producer_cpu_avg
            mem_consumer   = Num $r.mem_peak_mb
            mem_broker     = Num $r.broker_mem_peak_mb
            bytes_event    = (Num $r.allocated_mb) * 1e6 / (Num $r.measured_events)
            gc0_per_m      = (Num $r.gc_gen0) * 1e6 / (Num $r.events)
            gc1_per_m      = (Num $r.gc_gen1) * 1e6 / (Num $r.events)
            gc2_per_m      = (Num $r.gc_gen2) * 1e6 / (Num $r.events)
            valid          = $false
            motivo         = ''
            tukey          = $false
        }
    }
}

# Combinacao: broker x modo x carga, dentro do perfil, da frequencia e de um
# mesmo lado de uma medicao de decisao, quando houver.
function ComboKey($r) { "$($r.profile)|$($r.hz)|$($r.decisao)|$($r.lado)|$($r.broker)|$($r.mode)|$($r.rate)" }

# --- 2. validade -------------------------------------------------------------

# Resumo de referencia: o mais frequente entre as rodadas com a mesma entrada
# (carga, frequencia, trecho da corrida e duracao), em todos os brokers e modos.
$reference = @{}
foreach ($g in ($rows | Group-Object { "$($_.rate)|$($_.hz)|$($_.window)|$($_.seconds)" })) {
    $reference[$g.Name] = ($g.Group | Group-Object digest | Sort-Object Count -Descending | Select-Object -First 1).Name
}

foreach ($r in $rows) {
    $inputKey = "$($r.rate)|$($r.hz)|$($r.window)|$($r.seconds)"
    $r.motivo = if ([double]::IsNaN($r.jitter)) { 'sem_relatorio' }
        elseif ($r.jitter -gt $JitterCeilingMs) { 'atraso_envio' }
        elseif ($r.digest -ne $reference[$inputKey]) { 'conferencia' }
        elseif ($r.incomplete -gt 0) { 'registro_incompleto' }
        elseif ($r.dropped -gt 0) { 'persistencia' }
        else { '' }
    $r.valid = $r.motivo -eq ''
}

# --- 3. Tukey ---------------------------------------------------------------

foreach ($g in ($rows | Where-Object valid | Group-Object { ComboKey $_ })) {
    if ($g.Count -lt $MinRunsForTukey) { continue }
    $sorted = [double[]]@($g.Group.p99 | Sort-Object)
    $q1 = Quantile $sorted 0.25
    $q3 = Quantile $sorted 0.75
    $iqr = $q3 - $q1
    foreach ($r in $g.Group) {
        if ($r.p99 -lt $q1 - 1.5 * $iqr -or $r.p99 -gt $q3 + 1.5 * $iqr) { $r.tukey = $true }
    }
}

# --- 4. resumo por combinacao --------------------------------------------------

$summary = foreach ($g in ($rows | Group-Object { ComboKey $_ } | Sort-Object { $_.Group[0].profile }, { $_.Group[0].broker }, { $_.Group[0].mode }, { $_.Group[0].rate })) {
    $first = $g.Group[0]
    $kept = @($g.Group | Where-Object { $_.valid -and -not $_.tukey })
    $techFailures = @($g.Group | Where-Object { $_.motivo -eq 'sem_relatorio' }).Count
    $judged = $g.Count - $techFailures
    $validCount = @($g.Group | Where-Object valid).Count

    $medThroughput = Median $kept.throughput
    $medP99 = Median $kept.p99

    # Sustentada: maioria valida (regras 2 a 5), vazao mediana >= 99% da carga
    # e P99 mediano abaixo de 50 ms (ANALISE.md, secao 5).
    $majorityValid = $judged -gt 0 -and $validCount * 2 -gt $judged
    $sustained = $majorityValid -and $medThroughput -ge $SaturationThroughput * $first.rate -and $medP99 -lt $SaturationP99Ms
    $failed = if ($sustained) { '' }
        elseif (-not $majorityValid) { 'maioria_invalida' }
        elseif (-not ($medThroughput -ge $SaturationThroughput * $first.rate)) { 'vazao' }
        else { 'p99' }

    $alone = @($g.Group | Where-Object { $_.valid -and $_.throughput -ge $SaturationThroughput * $_.rate -and $_.p99 -lt $SaturationP99Ms }).Count

    $reasons = ($g.Group | Where-Object { -not $_.valid } | Group-Object motivo | ForEach-Object { "$($_.Name)=$($_.Count)" }) -join ';'

    [pscustomobject]@{
        perfil = $first.profile; hz = $first.hz; decisao = $first.decisao; lado = $first.lado
        broker = $first.broker; modo = $first.mode; carga = $first.rate
        rodadas = $g.Count; validas = $validCount; invalidas = ($reasons); descartadas_tukey = @($g.Group | Where-Object tukey).Count
        usadas = $kept.Count
        vazao_mediana = Fmt $medThroughput '0'; vazao_dp = Fmt (StdDev $kept.throughput) '0'
        p50_ms = Fmt (Median $kept.p50); p95_ms = Fmt (Median $kept.p95)
        p99_ms = Fmt $medP99; p99_dp = Fmt (StdDev $kept.p99); max_ms = Fmt (Median $kept.max)
        cpu_consumidor = Fmt (Median $kept.cpu_consumer) '0.0'; cpu_broker = Fmt (Median $kept.cpu_broker) '0.0'
        cpu_produtor = Fmt (Median $kept.cpu_producer) '0.0'
        mem_consumidor_mb = Fmt (Median $kept.mem_consumer) '0'; mem_broker_mb = Fmt (Median $kept.mem_broker) '0'
        bytes_por_evento = Fmt (Median $kept.bytes_event) '0.0'
        gc0_por_milhao = Fmt (Median $kept.gc0_per_m) '0.00'; gc1_por_milhao = Fmt (Median $kept.gc1_per_m) '0.00'
        gc2_por_milhao = Fmt (Median $kept.gc2_per_m) '0.00'
        sustentada = $sustained; falhou_em = $failed; rodadas_que_cumprem = "$alone/$($g.Count)"
        poucas_rodadas = ($validCount -lt $MinRunsForTukey)
    }
}

# --- 5. ponto de saturacao ---------------------------------------------------

$saturation = foreach ($g in ($summary | Group-Object { "$($_.perfil)|$($_.hz)|$($_.decisao)|$($_.lado)|$($_.broker)|$($_.modo)" })) {
    $loads = @($g.Group | Sort-Object carga)
    $point = 0
    $irregular = $false
    $stopped = $false
    foreach ($l in $loads) {
        if (-not $stopped -and $l.sustentada) { $point = $l.carga }
        elseif (-not $stopped) { $stopped = $true }
        elseif ($l.sustentada) { $irregular = $true }
    }
    $first = $loads[0]
    [pscustomobject]@{
        perfil = $first.perfil; hz = $first.hz; decisao = $first.decisao; lado = $first.lado
        broker = $first.broker; modo = $first.modo
        ponto_saturacao = $point
        irregular = $irregular
        cargas = ($loads | ForEach-Object { "$($_.carga):$(if ($_.sustentada) { 'ok' } else { $_.falhou_em })" }) -join ' '
    }
}

# --- 6. comparacoes entre modos -----------------------------------------------

$modes = foreach ($g in ($rows | Where-Object { $_.valid -and -not $_.tukey } |
        Group-Object { "$($_.profile)|$($_.hz)|$($_.decisao)|$($_.lado)|$($_.broker)|$($_.rate)" })) {
    $byMode = @{}
    foreach ($m in ($g.Group | Group-Object mode)) { $byMode[$m.Name] = [double[]]@($m.Group.p99) }
    $kw = KruskalWallis $byMode
    $first = $g.Group[0]
    $direct = $g.Group | Where-Object mode -eq 'direct'

    foreach ($m in ($g.Group | Group-Object mode | Where-Object Name -ne 'direct')) {
        $effectP99 = [double]::NaN
        $effectThroughput = [double]::NaN
        if ($direct) {
            $effectP99 = (Median $m.Group.p99) / (Median $direct.p99) - 1
            $effectThroughput = (Median $m.Group.throughput) / (Median $direct.throughput) - 1
        }
        $relevant = [math]::Abs($effectP99) -ge $RelevantP99 -or [math]::Abs($effectThroughput) -ge $RelevantThroughput

        [pscustomobject]@{
            perfil = $first.profile; hz = $first.hz; decisao = $first.decisao; lado = $first.lado
            broker = $first.broker; carga = $first.rate; modo = $m.Name
            efeito_p99 = Fmt $effectP99 '0.0%'; efeito_vazao = Fmt $effectThroughput '0.0%'
            relevante = if ($direct) { $relevant } else { '' }
            kruskal_h = Fmt $kw.H '0.00'; kruskal_p = Fmt $kw.P '0.0000'; modos_no_teste = $byMode.Keys.Count
        }
    }
}

# --- saida -------------------------------------------------------------------

$prefix = Join-Path $root $Out
$rows | Select-Object source, run_id, profile, hz, decisao, lado, broker, mode, rate, valid, motivo, tukey, p99, jitter |
    Export-Csv "$prefix-rodadas.csv" -NoTypeInformation -Encoding UTF8
$summary | Export-Csv "$prefix-resumo.csv" -NoTypeInformation -Encoding UTF8
$saturation | Export-Csv "$prefix-saturacao.csv" -NoTypeInformation -Encoding UTF8
@($modes) | Export-Csv "$prefix-modos.csv" -NoTypeInformation -Encoding UTF8

$invalid = @($rows | Where-Object { -not $_.valid }).Count
$discarded = @($rows | Where-Object tukey).Count
Write-Host "$($rows.Count) rodadas: $invalid invalidas, $discarded descartadas por Tukey, $(@($summary).Count) combinacoes."
Write-Host "Saida: $prefix-{rodadas,resumo,saturacao,modos}.csv"
