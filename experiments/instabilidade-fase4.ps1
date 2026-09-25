<#
.SYNOPSIS
Investigacao da fase 4 (docs/DESENVOLVIMENTO.md): de onde vem a instabilidade
do Kafka a 100 e 200 mil ev/s.

.DESCRIPTION
Todas as rodadas com linha do tempo de 100 ms (-Trace): produtor e
consumidor gravam em results/traces o atraso de emissao, as confirmacoes do
broker, a latencia maxima, as pausas de coleta de lixo e o pool de threads;
o log de coleta de lixo do broker vai junto. analysis/instabilidade.ps1
cruza os tres.

  100 mil: Direct e Channels.
  200 mil: Direct, Channels e Direct com a fila do produtor em 1 milhao,
           repetindo a comparacao da fase 2 (IMPLEMENTACAO, secao 8.4).

Perfil padrao, 100 Hz, protocolo da matriz. Os lados sao intercalados em cada
repeticao.

.EXAMPLE
./instabilidade-fase4.ps1
./instabilidade-fase4.ps1 -Reps 1 -Seconds 20   # ensaio curto
#>
param(
    [int]$Reps = 5,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [double]$Hz = 100,
    [double]$WindowStart = 600,
    [string]$Out = 'results/fase4-runs.csv',
    [string]$ConsumerReport = 'results/fase4-consumer.csv',
    [string]$ProducerReport = 'results/fase4-producer.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root $Out
$commit = Get-GitCommit

Assert-NoDash

$plan = @(
    @{ Rate = 100000; Sides = @(
        @{ Name = 'direct';            Mode = 'direct';   Producer = @() },
        @{ Name = 'channels';          Mode = 'channels'; Producer = @() }) },
    @{ Rate = 200000; Sides = @(
        @{ Name = 'direct';            Mode = 'direct';   Producer = @() },
        @{ Name = 'channels';          Mode = 'channels'; Producer = @() },
        @{ Name = 'direct-fila-1M';    Mode = 'direct';   Producer = @('--queue-max-messages', '1000000') }) }
)

$total = ($plan | ForEach-Object { $_.Sides.Count * $Reps } | Measure-Object -Sum).Sum
$index = 0
$failures = 0
$started = Get-Date

Write-Host "Fase 4: $total rodadas com linha do tempo | $Seconds s medidos | $Hz Hz | commit $commit" -ForegroundColor Cyan
Write-Host "--- broker: $(Use-Broker -Broker kafka -CpuMode cpuset)" -ForegroundColor Yellow

foreach ($p in $plan) {
    for ($rep = 1; $rep -le $Reps; $rep++) {
        foreach ($side in $p.Sides) {
            $index++
            $prefix = "[{0,2}/{1}]" -f $index, $total

            try {
                $row = Invoke-PitwallRun -Broker kafka -Mode $side.Mode -Rate $p.Rate `
                    -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $rep -Persist `
                    -Hz $Hz -WindowStart $WindowStart -Profile padrao -Trace `
                    -ProducerExtraArgs $side.Producer `
                    -ConsumerReport $ConsumerReport -ProducerReport $ProducerReport -Commit $commit
            }
            catch {
                $row = $null
                Write-Warning $_.Exception.Message
            }

            if ($null -eq $row) {
                $failures++
                Write-Host "$prefix $($side.Name) @ $($p.Rate) rep $rep`: FALHOU" -ForegroundColor Red
                continue
            }

            $row | Add-Member -NotePropertyName lado -NotePropertyValue $side.Name
            $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

            Write-Host ("{0} {1,-15} @{2,7}  vazao={3,9:N0}  P99={4,8:N2} ms  atraso={5} ms  rodada={6}" -f `
                $prefix, $side.Name, $p.Rate, [double]$row.throughput,
                ([double]$row.p99_us / 1000), $row.producer_jitter_ms, $row.run_id)
        }
    }
}

$elapsed = (Get-Date) - $started
Write-Host "Concluido: $($total - $failures) de $total rodadas em $([int]$elapsed.TotalMinutes) min. Resultado: $outPath" -ForegroundColor Cyan
