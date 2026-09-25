<#
.SYNOPSIS
Segunda bateria da fase 4: os arquivos do topico anterior, apagados no meio
da medicao, explicam os picos do Kafka?

.DESCRIPTION
A primeira bateria (instabilidade-fase4.ps1) achou picos de 200 ms a 2 s
concentrados cerca de 48 s depois do inicio das rodadas -- quando o broker
apaga os arquivos do topico excluido pela rodada anterior
(log.segment.delete.delay.ms = 60 s) -- e mostrou que o log do Kafka ficava na
camada do container, nao no volume. Com o log no volume (compose corrigido),
esta bateria compara:

  exclusao-padrao    arquivos apagados 60 s depois da exclusao, ou seja,
                     durante a rodada seguinte;
  exclusao-imediata  apagados na hora, e a rodada so comeca depois que eles
                     sumiram (Set-KafkaDeleteDelay -Ms 0).

Direct e Channels a 200 mil ev/s, 5 rodadas de cada por condicao, com linha
do tempo (inclui paginas sujas e PSI da VM no consumidor).

.EXAMPLE
./instabilidade-fase4b.ps1
#>
param(
    [int]$Reps = 5,
    [int]$Rate = 200000,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [string]$Out = 'results/fase4b-runs.csv',
    [string]$ConsumerReport = 'results/fase4b-consumer.csv',
    [string]$ProducerReport = 'results/fase4b-producer.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root $Out
$commit = Get-GitCommit

Assert-NoDash

$conditions = @(
    @{ Name = 'exclusao-padrao';   DelayMs = -1 },
    @{ Name = 'exclusao-imediata'; DelayMs = 0 }
)
$modes = @('direct', 'channels')
$total = $conditions.Count * $modes.Count * $Reps
$index = 0
$started = Get-Date

Write-Host "Fase 4b: $total rodadas a $Rate ev/s | commit $commit" -ForegroundColor Cyan
Write-Host "--- broker: $(Use-Broker -Broker kafka -CpuMode cpuset)" -ForegroundColor Yellow

try {
    foreach ($condition in $conditions) {
        Set-KafkaDeleteDelay -Ms $condition.DelayMs
        Write-Host "=== $($condition.Name)" -ForegroundColor Yellow

        for ($rep = 1; $rep -le $Reps; $rep++) {
            foreach ($mode in $modes) {
                $index++
                $row = $null
                try {
                    $row = Invoke-PitwallRun -Broker kafka -Mode $mode -Rate $Rate `
                        -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $rep -Persist `
                        -Hz 100 -WindowStart 600 -Profile padrao -Trace `
                        -ConsumerReport $ConsumerReport -ProducerReport $ProducerReport -Commit $commit
                }
                catch { Write-Warning $_.Exception.Message }

                if ($null -eq $row) { Write-Host "[$index/$total] $mode FALHOU" -ForegroundColor Red; continue }

                $row | Add-Member -NotePropertyName lado -NotePropertyValue "$mode-$($condition.Name)"
                $row | Add-Member -NotePropertyName exclusao -NotePropertyValue $condition.Name
                $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

                Write-Host ("[{0,2}/{1}] {2,-9} {3,-18} P99={4,8:N2} ms  atraso={5} ms" -f `
                    $index, $total, $mode, $condition.Name, ([double]$row.p99_us / 1000), $row.producer_jitter_ms)
            }
        }
    }
}
finally {
    # Volta ao padrao do broker, mesmo se a bateria falhar no meio.
    Set-KafkaDeleteDelay -Ms -1
}

Write-Host "Concluido em $([int]((Get-Date) - $started).TotalMinutes) min. Resultado: $outPath" -ForegroundColor Cyan
