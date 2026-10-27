<#
.SYNOPSIS
Matriz do cenario ajustado (fase 7 de docs/DESENVOLVIMENTO.md), versao
reduzida de 28/10/2026 (docs/ANALISE.md, tabela de mudancas).

.DESCRIPTION
So roda com o codigo da tag cenario-ajustado-v1.1 (a configuracao da v1, com
este roteiro reduzido): se src, infra ou experiments mudaram desde a tag, o
roteiro para antes da primeira rodada.

A pergunta da matriz ajustada e quanto a configuracao muda cada broker. O
efeito de Channels e Pipelines ja foi medido no cenario padrao (nenhum ganho);
aqui ele so e conferido em duas cargas por broker (hipotese H7d).

  rabbit  perfil ajustado: Direct em 10, 20, 40, 60 e 80 mil ev/s; Channels e
          Pipelines em 20 e 60 mil. 10 repeticoes: 90 rodadas.
  kafka   perfil ajustado: Direct em 10, 20, 40, 60, 100, 200, 300 e 400 mil;
          Channels e Pipelines em 20 e 200 mil. 10 repeticoes: 120 rodadas.
  vazao   perfil ajustado-vazao, so Kafka e so Direct, em 200, 300 e 400 mil.
          10 repeticoes: 30 rodadas.

Sem a bateria a 3,7 Hz: no cenario padrao a frequencia mudou menos de 10%.
Rodadas em ordem sorteada dentro de cada parte, com 10 s de aquecimento e
90 s de medicao, como no padrao.

.EXAMPLE
./matriz-ajustada.ps1 -Parte rabbit
./matriz-ajustada.ps1 -Parte vazao -Reps 1 -Seconds 20 -Ensaio   # fora da analise
#>
param(
    [Parameter(Mandatory)][ValidateSet('rabbit', 'kafka', 'vazao')][string]$Parte,
    [int]$Reps = 10,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [string]$Tag = 'cenario-ajustado-v1.1',
    # Ensaio: nao confere a tag e grava em arquivos ensaio-*.
    [switch]$Ensaio
)

# Sem $ErrorActionPreference = 'Stop': no PowerShell 5.1, o aviso que o
# consumidor escreve no stderr (libgssapi_krb5, inofensivo) viraria erro fatal
# na rodada (ver matriz-v3.ps1).
Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$git = "$env:ProgramFiles\Git\cmd\git.exe"

if (-not $Ensaio) {
    & $git -C $root rev-parse --verify --quiet "refs/tags/$Tag" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Tag $Tag nao existe. A matriz ajustada so roda com o cenario congelado." }

    $changed = @(& $git -C $root diff --name-only $Tag -- src infra experiments)
    $changed += @(& $git -C $root status --porcelain -- src infra experiments)
    if ($changed.Count -gt 0) {
        throw "Codigo diferente da tag ${Tag}:`n$($changed -join "`n")"
    }
}

switch ($Parte) {
    'rabbit' { $broker = 'rabbit'; $perfil = 'ajustado'
               $plan = [ordered]@{ direct = @(10000, 20000, 40000, 60000, 80000); channels = @(20000, 60000); pipelines = @(20000, 60000) } }
    'kafka'  { $broker = 'kafka'; $perfil = 'ajustado'
               $plan = [ordered]@{ direct = @(10000, 20000, 40000, 60000, 100000, 200000, 300000, 400000); channels = @(20000, 200000); pipelines = @(20000, 200000) } }
    'vazao'  { $broker = 'kafka'; $perfil = 'ajustado-vazao'
               $plan = [ordered]@{ direct = @(200000, 300000, 400000) } }
}

$name = if ($Parte -eq 'vazao') { 'matriz-ajustado-vazao-kafka' } else { "matriz-ajustado-$broker" }
if ($Ensaio) { $name = "ensaio-$name" }

$outPath = Join-Path $root "results/$name-runs.csv"
$commit = Get-GitCommit
Assert-NoDash

$runs = foreach ($mode in $plan.Keys) {
    foreach ($rate in $plan[$mode]) {
        for ($rep = 1; $rep -le $Reps; $rep++) {
            [pscustomobject]@{ Mode = $mode; Rate = $rate; Rep = $rep }
        }
    }
}
$runs = @($runs | Sort-Object { Get-Random })
$total = $runs.Count

Write-Host "Matriz ajustada, parte $Parte ($perfil): $total rodadas | $WarmupSeconds s + $Seconds s | commit $commit" -ForegroundColor Cyan
Write-Host "--- broker: $(Use-Broker -Broker $broker -CpuMode cpuset)" -ForegroundColor Yellow

$index = 0
$failures = 0
$started = Get-Date

foreach ($run in $runs) {
    $index++
    $prefix = "[{0,3}/{1}]" -f $index, $total

    try {
        $row = Invoke-PitwallRun -Broker $broker -Mode $run.Mode -Rate $run.Rate `
            -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $run.Rep -Persist `
            -Hz 100 -WindowStart 600 -Profile $perfil `
            -ConsumerReport "results/$name-consumer.csv" -ProducerReport "results/$name-producer.csv" -Commit $commit
    }
    catch {
        $row = $null
        Write-Warning $_.Exception.Message
    }

    if ($null -eq $row) {
        $failures++
        Write-Host "$prefix $broker-$($run.Mode) @ $($run.Rate) rep $($run.Rep): FALHOU" -ForegroundColor Red
        continue
    }

    $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

    $valid = 'ok'
    if ($row.producer_jitter_ms -and ([double]$row.producer_jitter_ms -gt 50)) { $valid = 'INVALIDA (jitter)' }

    Write-Host ("{0} {1,-18} @ {2,7:N0} rep {3}  vazao={4,9:N0}  P99={5,7:N2} ms  broker CPU={6,6}%  {7}" -f `
        $prefix, "$broker-$($run.Mode)", $run.Rate, $run.Rep, [double]$row.throughput,
        ([double]$row.p99_us / 1000), $row.broker_cpu_avg, $valid)
}

$elapsed = (Get-Date) - $started
Write-Host ""
Write-Host "Concluido em $($elapsed.ToString('hh\:mm\:ss')) | $failures falha(s) | $outPath" -ForegroundColor Cyan

# Na mesma carga, todos os modos precisam dar o mesmo digest.
if (Test-Path $outPath) {
    $rows = @(Import-Csv $outPath | Where-Object { $_.commit -eq $commit })
    $problems = 0
    foreach ($group in ($rows | Group-Object target_rate)) {
        if (@($group.Group | Group-Object digest_hash).Count -gt 1) {
            $problems++
            Write-Host "DIVERGENCIA a $($group.Name) ev/s" -ForegroundColor Red
        }
    }
    if ($problems -eq 0) { Write-Host "Verificacao cruzada: digest identico em todas as rodadas de cada carga." -ForegroundColor Green }
}
