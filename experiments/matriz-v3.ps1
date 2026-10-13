<#
.SYNOPSIS
Matriz v3 no cenario padrao (fase 6 de docs/DESENVOLVIMENTO.md): uma noite
por broker.

.DESCRIPTION
So roda com o codigo da tag cenario-padrao-v1.2 (a configuracao da v1; a
v1.1 corrigiu este roteiro e a v1.2 fixou as cargas): se src, infra ou
experiments mudaram desde a tag, o roteiro para antes da primeira rodada.

  Noite 1, RabbitMQ: 10, 20, 40, 60 e 80 mil ev/s x 3 modos x 10 = 150
           rodadas, mais 20 a 3,7 Hz (Direct, 20 e 60 mil).
  Noite 2, Kafka:    10, 20, 40, 60, 100, 200, 300 e 400 mil x 3 modos x 10
           = 240 rodadas, mais 20 a 3,7 Hz (Direct, 20 e 200 mil).

As cargas sao as fixadas em 24/09 (DESENVOLVIMENTO), mais uma acima do ponto
de saturacao em cada broker: no ensaio curto o RabbitMQ ainda ficou abaixo de
50 ms a 60 mil, e na varredura a 100 Hz o Kafka com Pipelines aguentou 300
mil. Sem essa carga, a saturacao ficaria so como "acima do maior ponto".

A bateria a 3,7 Hz grava em arquivos proprios: na mesma carga, a entrada e
diferente da de 100 Hz, e a verificacao cruzada do digest compara so rodadas
de entrada identica.

.EXAMPLE
./matriz-v3.ps1 -Noite 1
./matriz-v3.ps1 -Noite 2
./matriz-v3.ps1 -Noite 1 -Reps 1 -Seconds 20   # ensaio curto, fora da analise
#>
param(
    [Parameter(Mandatory)][ValidateSet(1, 2)][int]$Noite,
    [int]$Reps = 10,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [string]$Tag = 'cenario-padrao-v1.2'
)

# Sem $ErrorActionPreference = 'Stop': ele passa para o run-matrix, e no
# PowerShell 5.1 o aviso que o consumidor escreve no stderr (libgssapi_krb5,
# inofensivo) viraria erro fatal na rodada.
$root = Split-Path $PSScriptRoot -Parent
$git = "$env:ProgramFiles\Git\cmd\git.exe"

& $git -C $root rev-parse --verify --quiet "refs/tags/$Tag" | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Tag $Tag nao existe. A matriz so roda com o cenario padrao congelado (fase 3)." }

$changed = @(& $git -C $root diff --name-only $Tag -- src infra experiments)
$changed += @(& $git -C $root status --porcelain -- src infra experiments)
if ($changed.Count -gt 0) {
    throw "Codigo diferente da tag ${Tag}:`n$($changed -join "`n")"
}

$matrix = Join-Path $PSScriptRoot 'run-matrix.ps1'
$common = @{
    Reps = $Reps; Seconds = $Seconds; WarmupSeconds = $WarmupSeconds
    WindowStart = 600; Profile = 'padrao'
}

if ($Noite -eq 1) {
    $broker = 'rabbit'
    $rates = @(10000, 20000, 40000, 60000, 80000)
    $sensitivity = @(20000, 60000)
}
else {
    $broker = 'kafka'
    $rates = @(10000, 20000, 40000, 60000, 100000, 200000, 300000, 400000)
    $sensitivity = @(20000, 200000)
}

$name = "matriz-v3-$broker"

& $matrix @common -Brokers $broker -Rates $rates -Hz 100 `
    -Out "results/$name-runs.csv" `
    -ConsumerReport "results/$name-consumer.csv" `
    -ProducerReport "results/$name-producer.csv"

& $matrix @common -Brokers $broker -Modes direct -Rates $sensitivity -Hz 0 `
    -Out "results/$name-3hz-runs.csv" `
    -ConsumerReport "results/$name-3hz-consumer.csv" `
    -ProducerReport "results/$name-3hz-producer.csv"
