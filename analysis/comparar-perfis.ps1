<#
.SYNOPSIS
Compara o cenario ajustado com o padrao (docs/ANALISE.md, secao 10).

.DESCRIPTION
Le o <prefixo>-resumo.csv e o <prefixo>-saturacao.csv do analise.ps1 rodado
sobre as baterias dos dois perfis juntos e escreve:

  <Out>-efeito.csv     por broker x modo x carga x frequencia: mediana do
                       padrao, do ajustado e a diferenca relativa no P99, no
                       P50, na vazao e na CPU do consumidor, do broker e do
                       gerador, com a marca de relevante (10% vazao, 20% P99);
  <Out>-saturacao.csv  ponto de saturacao de cada perfil, lado a lado.

O perfil de comparacao e escolhido com -Ajustado (ajustado ou ajustado-vazao);
as linhas sem o mesmo broker, modo, carga e frequencia no padrao ficam de fora.

.EXAMPLE
./comparar-perfis.ps1 -Resumo results/analise-fase7 -Ajustado ajustado -Out results/fase7-latencia
#>
param(
    [string]$Resumo = 'results/analise-fase7',
    [ValidateSet('ajustado', 'ajustado-vazao')][string]$Ajustado = 'ajustado',
    [string]$Out = 'results/fase7-latencia',
    [double]$RelevantThroughput = 0.10,
    [double]$RelevantP99 = 0.20
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Num($v) {
    $n = 0.0
    if ([double]::TryParse(("$v" -replace ',', '.'), [System.Globalization.NumberStyles]::Float, $inv, [ref]$n)) { return $n }
    return [double]::NaN
}
function Rel($a, $b) {
    if ([double]::IsNaN($a) -or [double]::IsNaN($b) -or $b -eq 0) { return '' }
    return [math]::Round(($a - $b) / $b * 100, 1)
}

$rows = Import-Csv (Join-Path $root "$Resumo-resumo.csv")
$key = { param($r) "$($r.broker)|$($r.modo)|$($r.carga)|$($r.hz)" }
$base = @{}
foreach ($r in $rows | Where-Object perfil -eq 'padrao') { $base[(& $key $r)] = $r }

$efeito = foreach ($r in $rows | Where-Object perfil -eq $Ajustado) {
    $b = $base[(& $key $r)]
    if (-not $b) { continue }
    $dP99 = Rel (Num $r.p99_ms) (Num $b.p99_ms)
    $dVazao = Rel (Num $r.vazao_mediana) (Num $b.vazao_mediana)
    [pscustomobject]@{
        broker = $r.broker; modo = $r.modo; carga = $r.carga; hz = $r.hz
        p99_padrao = $b.p99_ms; p99_ajustado = $r.p99_ms; dif_p99_pct = $dP99
        p50_padrao = $b.p50_ms; p50_ajustado = $r.p50_ms; dif_p50_pct = Rel (Num $r.p50_ms) (Num $b.p50_ms)
        vazao_padrao = $b.vazao_mediana; vazao_ajustado = $r.vazao_mediana; dif_vazao_pct = $dVazao
        cpu_consumidor_padrao = $b.cpu_consumidor; cpu_consumidor_ajustado = $r.cpu_consumidor
        cpu_broker_padrao = $b.cpu_broker; cpu_broker_ajustado = $r.cpu_broker
        cpu_produtor_padrao = $b.cpu_produtor; cpu_produtor_ajustado = $r.cpu_produtor
        validas_padrao = $b.validas; validas_ajustado = $r.validas
        relevante = (($dP99 -ne '' -and [math]::Abs($dP99) -ge $RelevantP99 * 100) -or
                     ($dVazao -ne '' -and [math]::Abs($dVazao) -ge $RelevantThroughput * 100))
    }
}

$sat = Import-Csv (Join-Path $root "$Resumo-saturacao.csv")
$satBase = @{}
foreach ($s in $sat | Where-Object perfil -eq 'padrao') { $satBase["$($s.broker)|$($s.modo)|$($s.hz)"] = $s }
$saturacao = foreach ($s in $sat | Where-Object perfil -eq $Ajustado) {
    $b = $satBase["$($s.broker)|$($s.modo)|$($s.hz)"]
    [pscustomobject]@{
        broker = $s.broker; modo = $s.modo; hz = $s.hz
        saturacao_padrao = $b.ponto_saturacao; saturacao_ajustado = $s.ponto_saturacao
        cargas_ajustado = $s.cargas
    }
}

$prefix = Join-Path $root $Out
$efeito | Export-Csv "$prefix-efeito.csv" -NoTypeInformation -Encoding UTF8
$saturacao | Export-Csv "$prefix-saturacao.csv" -NoTypeInformation -Encoding UTF8
Write-Host "$(@($efeito).Count) combinacoes comparadas, $(@($efeito | Where-Object relevante).Count) com diferenca relevante."
Write-Host "Saida: $prefix-efeito.csv e $prefix-saturacao.csv"
