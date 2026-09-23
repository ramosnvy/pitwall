<#
.SYNOPSIS
Varredura de saturacao: encontra a carga em que cada arquitetura deixa de
acompanhar o produtor.

.DESCRIPTION
O experimento piloto. Os niveis de carga oficiais da matriz saem daqui, e nao
de um numero escolhido antes: se uma arquitetura satura em 40 mil ev/s, medi-la
a 100 mil so produz grafico de sistema quebrado.

Para cada uma das seis variantes, sobe a taxa ate a arquitetura falhar em um de
dois criterios: vazao abaixo de 90% da taxa alvo, ou P99 acima do teto.

.EXAMPLE
./sweep.ps1 -Rates 5000,10000,20000,50000,100000 -Seconds 10
#>
param(
    [string[]]$Brokers = @('kafka', 'rabbit'),
    [string[]]$Modes = @('direct', 'channels', 'pipelines'),
    [int[]]$Rates = @(5000, 10000, 20000, 50000, 100000, 200000),
    [int]$Seconds = 10,
    [int]$Partitions = 4,
    [double]$P99CeilingMs = 1000,
    [string]$Dataset = 'data/raw/9472/car_data.jsonl',
    [string]$Out = 'results/sweep.csv',
    [switch]$HostProcesses
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$summary = @()

Write-Host "Varredura de saturacao" -ForegroundColor Cyan
Write-Host "Taxas: $($Rates -join ', ') ev/s | $Seconds s por rodada | $Partitions faixas"
Write-Host ""

foreach ($broker in $Brokers) {
    foreach ($mode in $Modes) {
        $architecture = "$broker-$mode"
        $lastGood = 0

        Write-Host "--- $architecture ---" -ForegroundColor Yellow

        foreach ($rate in $Rates) {
            $row = Invoke-PitwallRun -Broker $broker -Mode $mode -Rate $rate `
                -Seconds $Seconds -Partitions $Partitions -Dataset $Dataset `
                -ConsumerReport $Out -ProducerReport 'results/sweep-producer.csv' -HostProcesses:$HostProcesses

            # A linha enriquecida (produtor, CPU e memoria de todos os modulos)
            # vai para um CSV proprio; o CSV do consumidor so tem o lado dele.
            if ($null -ne $row) {
                $row | Export-Csv -Path (Join-Path $root ($Out -replace '.csv$', '-runs.csv')) -Append -NoTypeInformation -Encoding UTF8
            }

            if ($null -eq $row) {
                Write-Host ("  {0,8} ev/s  FALHOU (sem resultado)" -f $rate) -ForegroundColor Red
                break
            }

            $throughput = [double]$row.throughput
            $p99Ms = [double]$row.p99_us / 1000.0
            $saturated = Test-RunSaturated -Row $row -Rate $rate -P99CeilingMs $P99CeilingMs

            $status = 'ok'
            $color = 'Green'
            if ($saturated) { $status = 'SATUROU'; $color = 'Red' }

            Write-Host ("  {0,8} ev/s  vazao={1,9:N0}  P99={2,8:N2} ms  {3}" -f `
                $rate, $throughput, $p99Ms, $status) -ForegroundColor $color

            if ($saturated) { break }

            $lastGood = $rate
        }

        $summary += [pscustomobject]@{
            architecture   = $architecture
            max_sustained  = $lastGood
        }

        Write-Host ""
    }
}

Write-Host "=== Vazao maxima sustentada ===" -ForegroundColor Cyan
$summary | Format-Table -AutoSize | Out-String | Write-Host

$summaryPath = Join-Path $root 'results/sweep-summary.csv'
$summary | Export-Csv -Path $summaryPath -NoTypeInformation -Encoding utf8
Write-Host "Resumo: $summaryPath"
Write-Host "Rodadas: $(Join-Path $root $Out)"
