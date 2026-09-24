<#
.SYNOPSIS
Experimento 2x2 das assimetrias da matriz 7e283c2 (docs/IMPLEMENTACAO.md).

.DESCRIPTION
Dois fatores, no RabbitMQ, modo direct:

  faixas     modulo (carro % 4, como na matriz: filas com 25/15/25/35% da carga)
             crc32  (mesma funcao do Kafka: 25% em cada fila)
  CPU        quota  (--cpus 4, como na matriz: cota CFS por periodo de 100 ms)
             cpuset (nucleos 4-7 fixos, sem cota: nada a estrangular)

O fatorial completo separa o efeito de cada fator e a interacao entre eles
(Jain, 1991). O modo direct basta: a matriz nao mostrou diferenca de P99 entre
os mecanismos no RabbitMQ, e o que se testa aqui e o broker.

A regra de decisao foi fixada antes de medir:
  - crc32 entra de qualquer forma, por ser correcao de justica da comparacao;
  - se cpuset reduzir o P99 em 20% ou mais em alguma carga, o protocolo passa
    a usar nucleos fixos nos brokers e o Kafka e conferido.

A troca de modo de CPU e feita com o container em execucao (docker update),
antes de cada rodada, entao as quatro celulas se intercalam em ordem aleatoria.
Ao final, o broker volta para a cota.

.EXAMPLE
./asymmetry-ab.ps1 -Rates 40000,60000 -Reps 5
#>
param(
    [int[]]$Rates = @(40000, 60000),
    [int]$Reps = 5,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [string]$Cpuset = '4-7',
    [string]$Out = 'results/asym-ab-runs.csv',
    [string]$ConsumerReport = 'results/asym-ab-consumer.csv',
    [string]$ProducerReport = 'results/asym-ab-producer.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root $Out
$commit = Get-GitCommit
$broker = 'pitwall-rabbitmq'

if ($commit -like '*-modificado') {
    Write-Warning "Codigo com alteracoes nao commitadas ($commit). Os resultados nao serao rastreaveis a um commit."
}

# O broker que nao esta sendo medido, e o painel, disputariam CPU.
$running = @(& docker ps --format '{{.Names}}')
$intruders = @($running | Where-Object { $_ -in @('pitwall-kafka', 'pitwall-grafana', 'pitwall-loki', 'pitwall-alloy', 'pitwall-replay') })
if ($intruders.Count -gt 0) {
    throw "Containers que nao fazem parte da medicao estao no ar: $($intruders -join ', '). Pare-os antes."
}

$runs = @()
foreach ($lane in @('modulo', 'crc32')) {
    foreach ($cpu in @('quota', 'cpuset')) {
        foreach ($rate in $Rates) {
            for ($rep = 1; $rep -le $Reps; $rep++) {
                $runs += [pscustomobject]@{ lane = $lane; cpu = $cpu; rate = $rate; rep = $rep }
            }
        }
    }
}
$runs = $runs | Sort-Object { Get-Random }

$total = $runs.Count
Write-Host "Experimento 2x2: faixas (modulo, crc32) x CPU do broker (quota, cpuset $Cpuset)" -ForegroundColor Cyan
Write-Host "$total rodadas | RabbitMQ direct | cargas $($Rates -join ', ') | $WarmupSeconds s + $Seconds s | commit $commit"
Write-Host "Tempo estimado: $([TimeSpan]::FromSeconds($total * ($Seconds + $WarmupSeconds + 30)).ToString('hh\:mm\:ss'))"
Write-Host ""

$index = 0
$failures = 0
$started = Get-Date

try {
    foreach ($run in $runs) {
        $index++
        $prefix = "[{0,2}/{1}]" -f $index, $total

        $mode = Set-BrokerCpuMode -Container $broker -Mode $run.cpu -Cpuset $Cpuset

        try {
            $row = Invoke-PitwallRun -Broker rabbit -Mode direct -Rate $run.rate `
                -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $run.rep -Persist `
                -LaneHash $run.lane -ConsumerReport $ConsumerReport -ProducerReport $ProducerReport `
                -Commit $commit -Profile legado
        }
        catch {
            $row = $null
            Write-Warning $_.Exception.Message
        }

        if ($null -eq $row) {
            $failures++
            Write-Host "$prefix $($run.lane)/$($run.cpu) @ $($run.rate) rep $($run.rep): FALHOU" -ForegroundColor Red
            continue
        }

        $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

        Write-Host ("{0} {1,-6} {2,-6} @ {3,6:N0} rep {4}  P99={5,7:N2} ms  P50={6,5:N2} ms  estrangulado={7,5}%  jitter={8} ms  [{9}]" -f `
            $prefix, $run.lane, $run.cpu, $run.rate, $run.rep, ([double]$row.p99_us / 1000), ([double]$row.p50_us / 1000), `
            $row.broker_throttled_pct, $row.producer_jitter_ms, $mode)
    }
}
finally {
    $restored = Set-BrokerCpuMode -Container $broker -Mode quota
    Write-Host ""
    Write-Host "Broker restaurado: $restored"
}

$elapsed = (Get-Date) - $started
Write-Host "Concluido em $($elapsed.ToString('hh\:mm\:ss')) | $failures falha(s)" -ForegroundColor Cyan

# A funcao de faixa muda qual fila recebe cada carro, mas nao a ordem de cada
# carro: o digest tem de ser o mesmo nas quatro celulas de cada carga.
if (Test-Path $outPath) {
    $rows = @(Import-Csv $outPath | Where-Object { $_.commit -eq $commit })
    foreach ($group in ($rows | Group-Object target_rate)) {
        $digests = @($group.Group | Group-Object digest_hash)
        if ($digests.Count -gt 1) {
            Write-Host "DIVERGENCIA de digest a $($group.Name) ev/s: $($digests.Count) valores" -ForegroundColor Red
        }
        else {
            Write-Host "Digest identico nas $($group.Count) rodadas a $($group.Name) ev/s." -ForegroundColor Green
        }
    }
}
