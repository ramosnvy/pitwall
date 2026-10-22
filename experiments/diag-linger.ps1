<#
.SYNOPSIS
Diagnostico da fase 7: o linger.ms 0 do Kafka piorou a latencia no ensaio.

.DESCRIPTION
No ensaio curto do perfil ajustado (22/10/2026), o Kafka com linger.ms 0 teve
P99 de 0,7 a 2,1 s a 10 mil ev/s (5,8 ms no padrao) e o broker com cerca de
1,8 nucleo de CPU. Mas cada rodada a 10 mil veio logo depois de uma a 400 mil,
que desabou. Este roteiro separa as duas explicacoes: Kafka sozinho, modo
Direct, a 10 e 100 mil ev/s, com linger.ms 0, 1 e 5 (o padrao, de controle),
intercalados em cada repeticao. Exploratorio: fora da analise da matriz.

.EXAMPLE
./diag-linger.ps1
#>
param(
    [int]$Reps = 2,
    [int]$Seconds = 30,
    [int]$WarmupSeconds = 10,
    [int[]]$Rates = @(10000, 100000),
    [string[]]$Lingers = @('0', '1', '5'),
    [string]$Out = 'results/diag-linger-runs.csv',
    [string]$ConsumerReport = 'results/diag-linger-consumer.csv',
    [string]$ProducerReport = 'results/diag-linger-producer.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root $Out
$commit = Get-GitCommit

Assert-NoDash
Write-Host "--- broker: $(Use-Broker -Broker kafka -CpuMode cpuset)" -ForegroundColor Yellow

$total = $Rates.Count * $Lingers.Count * $Reps
$index = 0
foreach ($rate in $Rates) {
    for ($rep = 1; $rep -le $Reps; $rep++) {
        foreach ($linger in $Lingers) {
            $index++
            try {
                $row = Invoke-PitwallRun -Broker kafka -Mode direct -Rate $rate `
                    -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $rep -Persist `
                    -Hz 100 -WindowStart 600 -Profile padrao `
                    -ProducerExtraArgs @('--linger-ms', $linger) -ConsumerExtraArgs @() `
                    -ConsumerReport $ConsumerReport -ProducerReport $ProducerReport -Commit $commit
            }
            catch { $row = $null; Write-Warning $_.Exception.Message }

            if ($null -eq $row) { Write-Host "[$index/$total] linger $linger @ $rate rep $rep`: FALHOU" -ForegroundColor Red; continue }

            $row | Add-Member -NotePropertyName decisao -NotePropertyValue 'diag-linger'
            $row | Add-Member -NotePropertyName lado -NotePropertyValue "linger-$linger"
            $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

            Write-Host ("[{0,2}/{1}] linger {2} @ {3,7}  vazao={4,9:N0}  P50={5,7:N2}  P99={6,8:N2} ms  broker CPU={7}%  atraso={8} ms" -f `
                $index, $total, $linger, $rate, [double]$row.throughput, ([double]$row.p50_us / 1000),
                ([double]$row.p99_us / 1000), $row.broker_cpu_avg, $row.producer_jitter_ms)
        }
    }
}
