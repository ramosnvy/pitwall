<#
.SYNOPSIS
Teste de Kruskal-Wallis: o mecanismo interno (direct, channels, pipelines)
muda a metrica, dentro de cada broker e carga?

.DESCRIPTION
Kruskal-Wallis e o teste nao parametrico para comparar tres ou mais grupos
independentes. Nao pressupoe normalidade -- com cinco repeticoes por celula
nao ha como verifica-la -- e compara postos, o que o torna robusto a rodadas
atipicas.

Com tres grupos, a estatistica H segue aproximadamente uma qui-quadrado com 2
graus de liberdade, cuja funcao de sobrevivencia tem forma fechada:
p = exp(-H / 2). Com cinco observacoes por grupo a aproximacao e aceitavel mas
nao exata; valores de p proximos de 0,05 devem ser lidos com cautela.

Aplica as mesmas regras de validade do summarize.ps1 e usa apenas rodadas
validas.

.EXAMPLE
./mechanism-test.ps1 -In results/matrix-v2-runs.csv -Metric p99_us
#>
param(
    [string]$In = 'results/matrix-v2-runs.csv',
    [string[]]$Metrics = @('p99_us', 'p50_us', 'consumer_container_cpu_avg'),
    [double]$JitterCeilingMs = 50
)

$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Num($value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return [double]::NaN }
    return [double]::Parse($value, $inv)
}

function Get-KruskalWallis([hashtable]$groups) {
    # Postos da amostra combinada, com media nos empates.
    $all = foreach ($name in $groups.Keys) {
        foreach ($v in $groups[$name]) { [pscustomobject]@{ group = $name; value = $v } }
    }

    $sorted = @($all | Sort-Object value)
    $n = $sorted.Count
    $ranks = @{}
    $tieCorrection = 0.0
    $i = 0

    while ($i -lt $n) {
        $j = $i
        while ($j + 1 -lt $n -and $sorted[$j + 1].value -eq $sorted[$i].value) { $j++ }

        $avg = (($i + 1) + ($j + 1)) / 2.0
        $t = $j - $i + 1
        if ($t -gt 1) { $tieCorrection += ($t * $t * $t - $t) }

        for ($k = $i; $k -le $j; $k++) {
            $g = $sorted[$k].group
            if (-not $ranks.ContainsKey($g)) { $ranks[$g] = @() }
            $ranks[$g] += $avg
        }

        $i = $j + 1
    }

    $h = 0.0
    foreach ($g in $ranks.Keys) {
        $r = ($ranks[$g] | Measure-Object -Sum).Sum
        $h += ($r * $r) / $ranks[$g].Count
    }

    $h = (12.0 / ($n * ($n + 1))) * $h - 3 * ($n + 1)

    if ($tieCorrection -gt 0) {
        $h = $h / (1 - $tieCorrection / ($n * $n * $n - $n))
    }

    # Qui-quadrado com 2 graus de liberdade: P(X > h) = exp(-h/2).
    $p = [math]::Exp(-$h / 2)

    return [pscustomobject]@{ H = $h; p = $p; n = $n }
}

$rows = @(Import-Csv (Join-Path $root $In))

$reference = @{}
foreach ($g in ($rows | Group-Object target_rate)) {
    $reference[$g.Name] = ($g.Group | Group-Object digest_hash | Sort-Object Count -Descending | Select-Object -First 1).Name
}

$valid = @($rows | Where-Object {
    $j = Num $_.producer_jitter_ms
    -not [double]::IsNaN($j) -and $j -le $JitterCeilingMs -and $_.digest_hash -eq $reference[$_.target_rate]
})

foreach ($metric in $Metrics) {
    Write-Host ""
    Write-Host "=== $metric ===" -ForegroundColor Cyan

    foreach ($cell in ($valid | Group-Object { ($_.architecture -split '-')[0] }, target_rate |
        Sort-Object { ($_.Group[0].architecture -split '-')[0] }, { [int]$_.Group[0].target_rate })) {

        $broker = ($cell.Group[0].architecture -split '-')[0]
        $rate = [int]$cell.Group[0].target_rate

        $groups = @{}
        foreach ($mode in @('direct', 'channels', 'pipelines')) {
            $values = @($cell.Group | Where-Object { $_.architecture -eq "$broker-$mode" } |
                ForEach-Object { Num $_.$metric } | Where-Object { -not [double]::IsNaN($_) })
            if ($values.Count -gt 0) { $groups[$mode] = $values }
        }

        if ($groups.Count -lt 3 -or ($groups.Values | Where-Object { $_.Count -lt 2 })) {
            Write-Host ("{0,-9} {1,7}  repeticoes validas insuficientes" -f $broker, $rate)
            continue
        }

        $kw = Get-KruskalWallis $groups
        $verdict = 'sem diferenca'
        if ($kw.p -lt 0.05) { $verdict = 'DIFERENCA (p<0,05)' }

        $medians = foreach ($mode in @('direct', 'channels', 'pipelines')) {
            $s = @($groups[$mode] | Sort-Object)
            $m = $s[[math]::Floor($s.Count / 2)]
            "{0}={1:N1}" -f $mode.Substring(0, 3), $m
        }

        Write-Host ("{0,-9} {1,7}  n={2,2}  H={3,5:N2}  p={4:N4}  {5,-20} medianas: {6}" -f `
            $broker, $rate, $kw.n, $kw.H, $kw.p, $verdict, ($medians -join ' '))
    }
}
