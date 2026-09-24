<#
.SYNOPSIS
Analise do experimento 2x2 das assimetrias (experiments/asymmetry-ab.ps1).

.DESCRIPTION
Para cada carga, projeto fatorial 2^2 com replicacao (Jain, 1991, cap. 18):

  y = q0 + qA*xA + qB*xB + qAB*xA*xB + erro

  A = funcao de faixa  (-1 modulo, +1 crc32)
  B = CPU do broker    (-1 quota,  +1 cpuset)

Os efeitos sao calculados sobre a media de cada celula, e a variacao total e
repartida entre A, B, a interacao AB e o erro (variacao entre repeticoes). A
fracao de variacao explicada diz qual fator importa; o erro diz quanto e ruido.

Aplica as mesmas regras de validade do summarize.ps1 (jitter do produtor e
digest) e, ao final, a regra de decisao fixada antes de medir: se cpuset
reduzir o P99 em 20% ou mais em alguma carga, o protocolo muda.
#>
param(
    [string]$In = 'results/asym-ab-runs.csv',
    [string[]]$Metrics = @('p99_us', 'p50_us', 'broker_throttled_pct', 'broker_cpu_avg'),
    [double]$JitterCeilingMs = 50,
    [double]$DecisionThreshold = 0.20
)

$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Num($v) { if ([string]::IsNullOrWhiteSpace($v)) { return [double]::NaN }; [double]::Parse($v, $inv) }
function Mean($xs) { ($xs | Measure-Object -Average).Average }
function Median($xs) { $s = @($xs | Sort-Object); if ($s.Count -eq 0) { return [double]::NaN }; $m = [math]::Floor($s.Count / 2); if ($s.Count % 2) { $s[$m] } else { ($s[$m - 1] + $s[$m]) / 2 } }

$rows = @(Import-Csv (Join-Path $root $In))

$reference = @{}
foreach ($g in ($rows | Group-Object target_rate)) {
    $reference[$g.Name] = ($g.Group | Group-Object digest_hash | Sort-Object Count -Descending | Select-Object -First 1).Name
}

$valid = @($rows | Where-Object {
    $j = Num $_.producer_jitter_ms
    -not [double]::IsNaN($j) -and $j -le $JitterCeilingMs -and $_.digest_hash -eq $reference[$_.target_rate]
})

Write-Host "Rodadas: $($rows.Count) | validas: $($valid.Count)"
foreach ($g in ($rows | Group-Object target_rate)) {
    $d = @($g.Group | Group-Object digest_hash).Count
    Write-Host ("  {0} ev/s: {1} digest(s) distinto(s)" -f $g.Name, $d)
}

function CpuLevel($row) { if ($row.broker_cpu_mode -like 'quota*') { 'quota' } else { 'cpuset' } }

$decision = @()

foreach ($rateGroup in ($valid | Group-Object target_rate | Sort-Object { [int]$_.Name })) {
    $rate = [int]$rateGroup.Name
    Write-Host ""
    Write-Host "=== $rate ev/s ===" -ForegroundColor Cyan

    # Medianas por celula, para leitura direta.
    $table = foreach ($lane in 'modulo', 'crc32') {
        foreach ($cpu in 'quota', 'cpuset') {
            $cell = @($rateGroup.Group | Where-Object { $_.lane_hash -eq $lane -and (CpuLevel $_) -eq $cpu })
            [pscustomobject]@{
                faixas = $lane; cpu = $cpu; n = $cell.Count
                p99_ms = [math]::Round((Median ($cell | ForEach-Object { (Num $_.p99_us) / 1000 })), 2)
                p50_ms = [math]::Round((Median ($cell | ForEach-Object { (Num $_.p50_us) / 1000 })), 2)
                estrangulado_pct = [math]::Round((Median ($cell | ForEach-Object { Num $_.broker_throttled_pct })), 2)
                broker_cpu = [math]::Round((Median ($cell | ForEach-Object { Num $_.broker_cpu_avg })), 1)
            }
        }
    }
    $table | Format-Table -AutoSize | Out-String | Write-Host

    foreach ($metric in $Metrics) {
        $cells = @{}
        foreach ($lane in 'modulo', 'crc32') {
            foreach ($cpu in 'quota', 'cpuset') {
                $cells["$lane|$cpu"] = @($rateGroup.Group | Where-Object { $_.lane_hash -eq $lane -and (CpuLevel $_) -eq $cpu } |
                    ForEach-Object { Num $_.$metric } | Where-Object { -not [double]::IsNaN($_) })
            }
        }
        if (@($cells.Values | Where-Object { $_.Count -lt 2 }).Count -gt 0) {
            Write-Host "  $metric`: repeticoes insuficientes em alguma celula"
            continue
        }

        # Tabela de sinais: (xA, xB) para cada celula.
        $y1 = Mean $cells['modulo|quota']   # (-1,-1)
        $y2 = Mean $cells['crc32|quota']    # (+1,-1)
        $y3 = Mean $cells['modulo|cpuset']  # (-1,+1)
        $y4 = Mean $cells['crc32|cpuset']   # (+1,+1)

        $q0 = ( $y1 + $y2 + $y3 + $y4) / 4
        $qA = (-$y1 + $y2 - $y3 + $y4) / 4
        $qB = (-$y1 - $y2 + $y3 + $y4) / 4
        $qAB = ( $y1 - $y2 - $y3 + $y4) / 4

        # Reparticao da variacao (Jain, 2^2 r).
        $r = [math]::Min([math]::Min($cells['modulo|quota'].Count, $cells['crc32|quota'].Count), [math]::Min($cells['modulo|cpuset'].Count, $cells['crc32|cpuset'].Count))
        $sse = 0.0
        foreach ($k in $cells.Keys) { $m = Mean $cells[$k]; foreach ($v in $cells[$k]) { $sse += ($v - $m) * ($v - $m) } }
        $ssa = 4 * $r * $qA * $qA
        $ssb = 4 * $r * $qB * $qB
        $ssab = 4 * $r * $qAB * $qAB
        $sst = $ssa + $ssb + $ssab + $sse
        $pct = { param($x) if ($sst -gt 0) { (100 * $x / $sst).ToString('0.0', $inv) } else { '-' } }

        Write-Host ("  {0,-22} media={1,10:N2}  faixas={2,9:N2}  cpu={3,9:N2}  interacao={4,9:N2}   variacao explicada: faixas {5}%  cpu {6}%  interacao {7}%  erro {8}%" -f `
            $metric, $q0, $qA, $qB, $qAB, (& $pct $ssa), (& $pct $ssb), (& $pct $ssab), (& $pct $sse))

        if ($metric -eq 'p99_us') {
            # Efeito relativo do cpuset em cada nivel de faixa, pela mediana.
            foreach ($lane in 'modulo', 'crc32') {
                $q = Median $cells["$lane|quota"]
                $c = Median $cells["$lane|cpuset"]
                $delta = ($c - $q) / $q
                $decision += [pscustomobject]@{ carga = $rate; faixas = $lane; p99_quota_ms = [math]::Round($q / 1000, 2); p99_cpuset_ms = [math]::Round($c / 1000, 2); variacao = $delta.ToString('+0.0%;-0.0%', $inv) }
            }
        }
    }
}

Write-Host ""
Write-Host "Regra de decisao (fixada antes de medir): cpuset muda o protocolo se reduzir o P99 em $($DecisionThreshold.ToString('0%', $inv)) ou mais." -ForegroundColor Cyan
$decision | Format-Table -AutoSize | Out-String | Write-Host

$hit = @($decision | Where-Object { [double]::Parse($_.variacao.TrimEnd('%'), $inv) / 100 -le -$DecisionThreshold })
if ($hit.Count -gt 0) {
    Write-Host "Resultado: cpuset reduz o P99 acima do limiar em $($hit.Count) celula(s). O protocolo passa a usar nucleos fixos nos brokers." -ForegroundColor Yellow
}
else {
    Write-Host "Resultado: cpuset nao atinge o limiar. O protocolo mantem a cota." -ForegroundColor Green
}
