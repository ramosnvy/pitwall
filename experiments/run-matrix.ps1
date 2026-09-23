<#
.SYNOPSIS
Executa a matriz completa de experimentos: 2 brokers x 3 mecanismos x N cargas
x R repeticoes.

.DESCRIPTION
A ordem das rodadas e embaralhada por padrao. Executar todas as repeticoes de
uma arquitetura em sequencia faria qualquer deriva do ambiente (aquecimento,
outro processo, termica) cair inteira sobre ela e virar diferenca de
arquitetura no resultado.

Os niveis de carga devem vir da varredura de saturacao (sweep.ps1), escolhidos
de modo que pelo menos um fique acima do joelho de cada arquitetura.

.EXAMPLE
./run-matrix.ps1 -Rates 10000,25000,50000 -Reps 5 -Seconds 90 -Persist
#>
param(
    [string[]]$Brokers = @('kafka', 'rabbit'),
    [string[]]$Modes = @('direct', 'channels', 'pipelines'),
    [int[]]$Rates = @(10000, 25000, 50000),
    [int]$Reps = 5,
    [int]$Seconds = 60,
    [int]$Partitions = 4,
    [double]$SyntheticCostUs = 0,
    [switch]$Persist,
    [switch]$NoShuffle,
    [string]$Dataset = 'data/raw/9472/car_data.jsonl',
    [string]$Out = 'results/matrix.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent

# Monta a lista completa de rodadas antes de executar qualquer uma.
$runs = @()
foreach ($broker in $Brokers) {
    foreach ($mode in $Modes) {
        foreach ($rate in $Rates) {
            for ($rep = 1; $rep -le $Reps; $rep++) {
                $runs += [pscustomobject]@{
                    broker = $broker; mode = $mode; rate = $rate; rep = $rep
                }
            }
        }
    }
}

if (-not $NoShuffle) {
    $runs = $runs | Sort-Object { Get-Random }
}

$total = $runs.Count
$estimate = [TimeSpan]::FromSeconds($total * ($Seconds + 25))

Write-Host "Matriz de experimentos" -ForegroundColor Cyan
Write-Host "$total rodadas | $Seconds s de medicao | ordem $(if ($NoShuffle) { 'sequencial' } else { 'aleatoria' })"
Write-Host "Tempo estimado: $($estimate.ToString('hh\:mm\:ss'))"
Write-Host "Inicio: $(Get-Date -Format 'HH:mm:ss')"
Write-Host ""

$index = 0
$failures = 0
$started = Get-Date

foreach ($run in $runs) {
    $index++
    $prefix = "[{0,3}/{1}]" -f $index, $total

    $row = Invoke-PitwallRun -Broker $run.broker -Mode $run.mode -Rate $run.rate `
        -Seconds $Seconds -Partitions $Partitions -Replication $run.rep `
        -Persist:$Persist -Dataset $Dataset -ConsumerReport $Out `
        -ProducerReport 'results/matrix-producer.csv' -SyntheticCostUs $SyntheticCostUs

    if ($null -eq $row) {
        $failures++
        Write-Host "$prefix $($run.broker)-$($run.mode) @ $($run.rate) rep $($run.rep): FALHOU" -ForegroundColor Red
        continue
    }

    $p99Ms = [double]$row.p99_us / 1000.0

    Write-Host ("{0} {1,-18} @ {2,7:N0} rep {3}  vazao={4,9:N0}  P99={5,7:N2} ms" -f `
        $prefix, "$($run.broker)-$($run.mode)", $run.rate, $run.rep, `
        [double]$row.throughput, $p99Ms)
}

$elapsed = (Get-Date) - $started

Write-Host ""
Write-Host "Concluido em $($elapsed.ToString('hh\:mm\:ss')) | $failures falha(s)" -ForegroundColor Cyan
Write-Host "Resultados: $(Join-Path $root $Out)"

# A verificacao cruzada: todas as rodadas com a mesma entrada devem ter
# produzido o mesmo digest. Divergencia significa perda de evento ou quebra de
# ordem, e as rodadas divergentes nao podem entrar na analise.
$rows = @(Import-Csv (Join-Path $root $Out))
$digests = $rows | Group-Object digest_hash | Sort-Object Count -Descending

if ($digests.Count -gt 1) {
    Write-Host ""
    Write-Host "ATENCAO: digests diferentes entre rodadas." -ForegroundColor Yellow
    Write-Host "Esperado quando as cargas variam (entradas diferentes), mas rodadas"
    Write-Host "com a mesma carga e o mesmo numero de eventos devem coincidir."
    $digests | Select-Object -First 5 Name, Count | Format-Table -AutoSize | Out-String | Write-Host
}
