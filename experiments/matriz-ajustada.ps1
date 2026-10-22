<#
.SYNOPSIS
Matriz do cenario ajustado (fase 7 de docs/DESENVOLVIMENTO.md).

.DESCRIPTION
So roda com o codigo da tag cenario-ajustado-v1: se src, infra ou experiments
mudaram desde a tag, o roteiro para antes da primeira rodada. Tres partes,
na ordem em que a sessao continua as roda:

  rabbit  perfil ajustado (baixa latencia): mesmas cargas, modos e repeticoes
          da matriz v3 do padrao (10 a 80 mil ev/s, 150 rodadas), mais 20 a
          3,7 Hz (Direct, 20 e 60 mil).
  kafka   perfil ajustado (baixa latencia): 10 a 400 mil ev/s, 240 rodadas,
          mais 20 a 3,7 Hz (Direct, 20 e 200 mil).
  vazao   perfil ajustado-vazao, so Kafka, nas tres cargas mais altas (200,
          300 e 400 mil) x 3 modos x 10 = 90 rodadas. Lotes maiores so mudam
          algo perto da saturacao; nas cargas baixas so aumentariam a latencia.

Os perfis estao em Get-ProfileArgs (experiments/Pitwall.Runner.psm1) e em
docs/AUDITORIA-CONFIG.md. As cargas sao as mesmas do padrao, para comparar
combinacao a combinacao (docs/ANALISE.md, secao 10).

.EXAMPLE
./matriz-ajustada.ps1 -Parte rabbit
./matriz-ajustada.ps1 -Parte vazao -Reps 1 -Seconds 20 -Ensaio   # fora da analise
#>
param(
    [Parameter(Mandatory)][ValidateSet('rabbit', 'kafka', 'vazao')][string]$Parte,
    [int]$Reps = 10,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [string]$Tag = 'cenario-ajustado-v1',
    # Ensaio: nao confere a tag, usa so duas cargas e grava em arquivos ensaio-*.
    [switch]$Ensaio
)

# Sem $ErrorActionPreference = 'Stop': ele passa para o run-matrix, e no
# PowerShell 5.1 o aviso que o consumidor escreve no stderr (libgssapi_krb5,
# inofensivo) viraria erro fatal na rodada (ver matriz-v3.ps1).
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

$matrix = Join-Path $PSScriptRoot 'run-matrix.ps1'
$common = @{ Reps = $Reps; Seconds = $Seconds; WarmupSeconds = $WarmupSeconds; WindowStart = 600 }

switch ($Parte) {
    'rabbit' { $broker = 'rabbit'; $perfil = 'ajustado'
               $rates = @(10000, 20000, 40000, 60000, 80000); $sensitivity = @(20000, 60000) }
    'kafka'  { $broker = 'kafka'; $perfil = 'ajustado'
               $rates = @(10000, 20000, 40000, 60000, 100000, 200000, 300000, 400000); $sensitivity = @(20000, 200000) }
    'vazao'  { $broker = 'kafka'; $perfil = 'ajustado-vazao'
               $rates = @(200000, 300000, 400000); $sensitivity = @() }
}

$name = if ($Parte -eq 'vazao') { 'matriz-ajustado-vazao-kafka' } else { "matriz-ajustado-$broker" }
if ($Ensaio) {
    $name = "ensaio-$name"
    $rates = @($rates[0], $rates[-1])
    $sensitivity = @()
}

& $matrix @common -Profile $perfil -Brokers $broker -Rates $rates -Hz 100 `
    -Out "results/$name-runs.csv" `
    -ConsumerReport "results/$name-consumer.csv" `
    -ProducerReport "results/$name-producer.csv"

if ($sensitivity.Count -gt 0) {
    & $matrix @common -Profile $perfil -Brokers $broker -Modes direct -Rates $sensitivity -Hz 0 `
        -Out "results/$name-3hz-runs.csv" `
        -ConsumerReport "results/$name-3hz-consumer.csv" `
        -ProducerReport "results/$name-3hz-producer.csv"
}
