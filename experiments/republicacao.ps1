<#
.SYNOPSIS
Segundo cenario, passo A (fase 9 de docs/DESENVOLVIMENTO.md): republicacao
sobre a carga atual, com latencia de ponta a ponta medida pelo painel.

.DESCRIPTION
Cada rodada e como as da matriz (perfil padrao, 100 Hz, trecho a partir do
minuto 10, 10 s de aquecimento), mais a republicacao da saida no mesmo broker
e o painel. Combinacoes: broker x modo x saida (enriquecida ou resumida) x
carga, em ordem sorteada dentro de cada broker, um broker por vez.

Grava em results/<Nome>-runs.csv (uma linha por rodada, com as colunas e2e_*
do painel) e nos relatorios do consumidor, do produtor e do painel.

.EXAMPLE
./republicacao.ps1 -Nome ensaio-republicacao -Rates 20000 -Reps 1 -Seconds 20   # fumaca
./republicacao.ps1 -Nome republicacao-varredura -Rates 10000,20000,40000,60000 -Reps 3
#>
param(
    [string[]]$Brokers = @('rabbit', 'kafka'),
    [string[]]$Modes = @('direct', 'channels', 'pipelines'),
    [ValidateSet('enriquecida', 'resumida')][string[]]$Outputs = @('enriquecida', 'resumida'),
    [int[]]$Rates = @(20000),
    [int]$Reps = 3,
    # Saida resumida com cargas e repeticoes proprias: ela quase nao espera
    # confirmacao (fumaca de 27/10) e so confere a hipotese H5b. Vazio = as
    # mesmas da enriquecida.
    [int[]]$ResumidaRates = @(),
    [int]$ResumidaReps = 0,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [string]$Nome = 'republicacao'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root "results/$Nome-runs.csv"
$commit = Get-GitCommit

Assert-NoDash
if ($commit -like '*-modificado') {
    Write-Warning "Codigo com alteracoes nao commitadas ($commit)."
}

$runs = foreach ($broker in $Brokers) {
    $block = foreach ($mode in $Modes) {
        foreach ($output in $Outputs) {
            $outRates = if ($output -eq 'resumida' -and $ResumidaRates.Count -gt 0) { $ResumidaRates } else { $Rates }
            $outReps = if ($output -eq 'resumida' -and $ResumidaReps -gt 0) { $ResumidaReps } else { $Reps }
            foreach ($rate in $outRates) {
                for ($rep = 1; $rep -le $outReps; $rep++) {
                    [pscustomobject]@{ Broker = $broker; Mode = $mode; Output = $output; Rate = $rate; Rep = $rep }
                }
            }
        }
    }
    $block | Sort-Object { Get-Random }
}

$total = @($runs).Count
Write-Host "Segundo cenario: $total rodadas | $WarmupSeconds s + $Seconds s | commit $commit" -ForegroundColor Cyan

$index = 0
$failures = 0
$active = ''
$started = Get-Date

foreach ($run in $runs) {
    $index++
    $prefix = "[{0,3}/{1}]" -f $index, $total

    if ($run.Broker -ne $active) {
        Write-Host "--- broker: $(Use-Broker -Broker $run.Broker -CpuMode cpuset)" -ForegroundColor Yellow
        $active = $run.Broker
    }

    try {
        $row = Invoke-PitwallRun -Broker $run.Broker -Mode $run.Mode -Rate $run.Rate `
            -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $run.Rep -Persist `
            -Hz 100 -WindowStart 600 -Profile padrao -Republish $run.Output `
            -ConsumerReport "results/$Nome-consumer.csv" -ProducerReport "results/$Nome-producer.csv" `
            -PainelReport "results/$Nome-painel.csv" -Commit $commit
    }
    catch {
        $row = $null
        Write-Warning $_.Exception.Message
    }

    if ($null -eq $row) {
        $failures++
        Write-Host "$prefix $($run.Broker)-$($run.Mode) $($run.Output) @ $($run.Rate) rep $($run.Rep): FALHOU" -ForegroundColor Red
        continue
    }

    $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

    $valid = 'ok'
    if ($row.producer_jitter_ms -and ([double]$row.producer_jitter_ms -gt 50)) { $valid = 'INVALIDA (jitter)' }
    $e2e = if ($row.e2e_p99_us) { '{0,8:N2}' -f ([double]$row.e2e_p99_us / 1000) } else { '     ---' }

    Write-Host ("{0} {1,-18} {2,-11} @ {3,7:N0} rep {4}  vazao={5,9:N0}  P99 proc={6,7:N2} ms  P99 ponta a ponta={7} ms  esperas={8} ({9} ms)  {10}" -f `
        $prefix, "$($run.Broker)-$($run.Mode)", $run.Output, $run.Rate, $run.Rep, [double]$row.throughput,
        ([double]$row.p99_us / 1000), $e2e, $row.confirm_waits, $row.confirm_wait_ms, $valid)
}

$elapsed = (Get-Date) - $started
Write-Host ""
Write-Host "Concluido em $($elapsed.ToString('hh\:mm\:ss')) | $failures falha(s) | $outPath" -ForegroundColor Cyan

# Conferencia: na mesma carga e saida, todos os brokers e modos precisam dar o
# mesmo digest no consumidor e no painel.
if (Test-Path $outPath) {
    $rows = @(Import-Csv $outPath | Where-Object { $_.commit -eq $commit })
    foreach ($group in ($rows | Group-Object target_rate, republish)) {
        $proc = @($group.Group | Group-Object digest_hash).Count
        $e2e = @($group.Group | Group-Object e2e_digest_hash).Count
        $color = if ($proc -eq 1 -and $e2e -eq 1) { 'Green' } else { 'Red' }
        Write-Host "Conferencia $($group.Name): digest do consumidor $proc valor(es), do painel $e2e valor(es)" -ForegroundColor $color
    }
}
