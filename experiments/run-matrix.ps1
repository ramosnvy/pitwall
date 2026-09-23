<#
.SYNOPSIS
Executa a matriz de experimentos: brokers x mecanismos x cargas x repeticoes.

.DESCRIPTION
A ordem das rodadas e embaralhada por padrao. Executar todas as repeticoes de
uma arquitetura em sequencia faria qualquer deriva do ambiente (aquecimento,
outro processo, termica) cair inteira sobre ela e virar diferenca de
arquitetura no resultado.

A persistencia fica ligada por padrao: a arquitetura declarada no TCC1 inclui o
modulo de persistencia em PostgreSQL. Use -NoPersist para a bateria de controle
que isola o custo do banco.

Cada rodada gera uma linha no CSV consolidado (-Out), com consumidor, produtor e
metricas dos containers ligados pelo mesmo run_id, mais o commit do codigo que
a produziu.

Os niveis de carga vem da varredura de saturacao (docs/PILOTO.md).

.EXAMPLE
./run-matrix.ps1 -Rates 5000,10000,20000,30000 -Reps 5 -Seconds 90

.EXAMPLE
./run-matrix.ps1 -Brokers kafka -Rates 100000,200000,400000 -Reps 5 -Seconds 90
#>
param(
    [string[]]$Brokers = @('kafka', 'rabbit'),
    [string[]]$Modes = @('direct', 'channels', 'pipelines'),
    [int[]]$Rates = @(5000, 10000, 20000, 30000),
    [int]$Reps = 5,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [int]$Partitions = 4,
    [double]$SyntheticCostUs = 0,
    [switch]$NoPersist,
    [switch]$NoShuffle,
    [switch]$HostProcesses,
    [string]$Dataset = 'data/raw/9472/car_data.jsonl',
    [string]$Out = 'results/matrix-runs.csv',
    [string]$ConsumerReport = 'results/matrix-consumer.csv',
    [string]$ProducerReport = 'results/matrix-producer.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root $Out
$commit = Get-GitCommit

if ($commit -like '*-modificado') {
    Write-Warning "Codigo com alteracoes nao commitadas ($commit). Os resultados nao serao rastreaveis a um commit."
}

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
$perRun = $Seconds + $WarmupSeconds + 30
$estimate = [TimeSpan]::FromSeconds($total * $perRun)
$order = 'aleatoria'
if ($NoShuffle) { $order = 'sequencial' }
$persistLabel = 'ligada'
if ($NoPersist) { $persistLabel = 'desligada' }
$placementLabel = 'containers na rede Docker'
if ($HostProcesses) { $placementLabel = 'processos no host Windows' }

Write-Host "Matriz de experimentos" -ForegroundColor Cyan
Write-Host "$total rodadas | $WarmupSeconds s aquecimento + $Seconds s medicao | ordem $order | persistencia $persistLabel"
Write-Host "Clientes: $placementLabel"
Write-Host "Commit: $commit"
Write-Host "Tempo estimado: $($estimate.ToString('hh\:mm\:ss'))"
Write-Host "Inicio: $(Get-Date -Format 'HH:mm:ss')"
Write-Host ""

$index = 0
$failures = 0
$started = Get-Date

foreach ($run in $runs) {
    $index++
    $prefix = "[{0,3}/{1}]" -f $index, $total

    try {
        $row = Invoke-PitwallRun -Broker $run.broker -Mode $run.mode -Rate $run.rate `
            -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Partitions $Partitions `
            -Replication $run.rep -Persist:(-not $NoPersist) -Dataset $Dataset `
            -ConsumerReport $ConsumerReport -ProducerReport $ProducerReport `
            -SyntheticCostUs $SyntheticCostUs -Commit $commit -HostProcesses:$HostProcesses
    }
    catch {
        $row = $null
        Write-Warning $_.Exception.Message
    }

    if ($null -eq $row) {
        $failures++
        Write-Host "$prefix $($run.broker)-$($run.mode) @ $($run.rate) rep $($run.rep): FALHOU" -ForegroundColor Red
        continue
    }

    $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

    $p99Ms = [double]$row.p99_us / 1000.0
    $valid = 'ok'
    if ($row.producer_jitter_ms -and ([double]$row.producer_jitter_ms -gt 50)) { $valid = 'INVALIDA (jitter)' }

    Write-Host ("{0} {1,-18} @ {2,7:N0} rep {3}  vazao={4,9:N0}  P99={5,7:N2} ms  broker CPU={6,6}%  {7}" -f `
        $prefix, "$($run.broker)-$($run.mode)", $run.rate, $run.rep, `
        [double]$row.throughput, $p99Ms, $row.broker_cpu_avg, $valid)
}

$elapsed = (Get-Date) - $started

Write-Host ""
Write-Host "Concluido em $($elapsed.ToString('hh\:mm\:ss')) | $failures falha(s)" -ForegroundColor Cyan
Write-Host "Resultados: $outPath"

# Verificacao cruzada. Na mesma taxa, a entrada e identica em todas as rodadas
# (mesmo dataset, mesmo fator de frota, mesmo numero de eventos) -- entao as
# seis arquiteturas e todas as repeticoes precisam produzir o mesmo digest.
# Divergencia significa perda de evento ou quebra de ordem, e a rodada nao pode
# entrar na analise.
if (Test-Path $outPath) {
    $rows = @(Import-Csv $outPath | Where-Object { $_.commit -eq $commit })
    $problems = 0

    foreach ($group in ($rows | Group-Object target_rate)) {
        $digests = @($group.Group | Group-Object digest_hash)

        if ($digests.Count -gt 1) {
            $problems++
            Write-Host ""
            Write-Host "DIVERGENCIA a $($group.Name) ev/s: $($digests.Count) digests diferentes" -ForegroundColor Red
            foreach ($d in $digests) {
                $archs = ($d.Group | ForEach-Object { "$($_.architecture)#$($_.replication)" }) -join ', '
                Write-Host "  $($d.Name) ($($d.Count)x): $archs"
            }
        }
    }

    if ($problems -eq 0) {
        Write-Host "Verificacao cruzada: digest identico em todas as rodadas de cada carga." -ForegroundColor Green
    }
}
