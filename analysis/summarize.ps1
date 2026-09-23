<#
.SYNOPSIS
Consolida o CSV da matriz em uma linha por arquitetura e carga, aplicando as
regras de validade do projeto.

.DESCRIPTION
Uma rodada so entra na analise se:

  1. o jitter de emissao do produtor foi de no maximo 50 ms -- acima disso o
     gerador nao sustentou a taxa e a rodada nao mede a arquitetura;
  2. o digest e igual ao da maioria das rodadas da mesma carga -- divergencia
     significa evento perdido ou fora de ordem.

As estatisticas por celula usam a MEDIANA entre repeticoes validas, nao a
media: com cinco repeticoes e a variancia medida neste ambiente, uma rodada
atipica deslocaria a media inteira. Minimo e maximo do P99 acompanham a
mediana para mostrar a dispersao.

Janelas descartadas na persistencia NAO invalidam a rodada para latencia e
vazao; sao reportadas a parte, como limite do modulo de persistencia.

.EXAMPLE
./summarize.ps1 -In results/matrix-runs.csv -Out results/summary.csv
#>
param(
    [string]$In = 'results/matrix-runs.csv',
    [string]$Out = 'results/summary.csv',
    [double]$JitterCeilingMs = 50
)

$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Median([double[]]$values) {
    if ($values.Count -eq 0) { return [double]::NaN }
    $sorted = $values | Sort-Object
    $mid = [math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) { return [double]$sorted[$mid] }
    return ([double]$sorted[$mid - 1] + [double]$sorted[$mid]) / 2
}

function Num($value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return [double]::NaN }
    return [double]::Parse($value, $inv)
}

$rows = @(Import-Csv (Join-Path $root $In))

# Digest de referencia por carga: o da maioria das rodadas daquela carga.
$referenceDigest = @{}
foreach ($group in ($rows | Group-Object target_rate)) {
    $referenceDigest[$group.Name] = ($group.Group | Group-Object digest_hash |
        Sort-Object Count -Descending | Select-Object -First 1).Name
}

$classified = foreach ($r in $rows) {
    $reasons = @()

    if ((Num $r.producer_jitter_ms) -gt $JitterCeilingMs) { $reasons += 'jitter' }
    if ($r.digest_hash -ne $referenceDigest[$r.target_rate]) { $reasons += 'digest' }

    $r | Add-Member -NotePropertyName valid -NotePropertyValue ($reasons.Count -eq 0) -PassThru |
        Add-Member -NotePropertyName invalid_reason -NotePropertyValue ($reasons -join '+') -PassThru
}

$summary = foreach ($cell in ($classified | Group-Object architecture, target_rate)) {
    $all = @($cell.Group)
    $ok = @($all | Where-Object valid)
    $first = $all[0]

    $p99 = @($ok | ForEach-Object { (Num $_.p99_us) / 1000 })

    [pscustomobject][ordered]@{
        architecture       = $first.architecture
        target_rate        = [int]$first.target_rate
        runs               = $all.Count
        valid_runs         = $ok.Count
        invalid            = (($all | Where-Object { -not $_.valid } |
                               ForEach-Object { "#$($_.replication):$($_.invalid_reason)" }) -join ' ')
        throughput         = [math]::Round((Median @($ok | ForEach-Object { Num $_.throughput })), 0)
        mean_ms            = [math]::Round((Median @($ok | ForEach-Object { (Num $_.mean_us) / 1000 })), 2)
        p50_ms             = [math]::Round((Median @($ok | ForEach-Object { (Num $_.p50_us) / 1000 })), 2)
        p95_ms             = [math]::Round((Median @($ok | ForEach-Object { (Num $_.p95_us) / 1000 })), 2)
        p99_ms             = [math]::Round((Median $p99), 2)
        p99_min_ms         = if ($p99.Count) { [math]::Round(($p99 | Measure-Object -Minimum).Minimum, 2) } else { $null }
        p99_max_ms         = if ($p99.Count) { [math]::Round(($p99 | Measure-Object -Maximum).Maximum, 2) } else { $null }
        consumer_cpu_pct   = [math]::Round((Median @($ok | ForEach-Object { Num $_.cpu_avg })), 1)
        consumer_mem_mb    = [math]::Round((Median @($ok | ForEach-Object { Num $_.mem_peak_mb })), 0)
        broker_cpu_pct     = [math]::Round((Median @($ok | ForEach-Object { Num $_.broker_cpu_avg })), 1)
        broker_mem_mb      = [math]::Round((Median @($ok | ForEach-Object { Num $_.broker_mem_peak_mb })), 0)
        db_cpu_pct         = [math]::Round((Median @($ok | ForEach-Object { Num $_.db_cpu_avg })), 1)
        windows_dropped    = [math]::Round((Median @($ok | ForEach-Object { Num $_.windows_dropped })), 0)
    }
}

$summary = $summary | Sort-Object target_rate, architecture
$summary | Export-Csv -Path (Join-Path $root $Out) -NoTypeInformation -Encoding UTF8

$invalid = @($classified | Where-Object { -not $_.valid })
Write-Host "Rodadas: $($rows.Count) | validas: $($rows.Count - $invalid.Count) | invalidas: $($invalid.Count)"
foreach ($g in ($invalid | Group-Object invalid_reason)) {
    Write-Host "  $($g.Name): $($g.Count)"
}
Write-Host "Resumo: $(Join-Path $root $Out)"

$summary
