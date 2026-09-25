<#
.SYNOPSIS
Medicoes de decisao da fase 2 (docs/DESENVOLVIMENTO.md): fixam os valores
provisorios do perfil padrao antes de congelar o cenario.

.DESCRIPTION
Quatro comparacoes, cada uma com 3 rodadas por lado, intercaladas (A, B, A,
B...), modo Direct, 100 Hz, protocolo da matriz (10 s de aquecimento e 90 s de
medicao). A regra de decisao de cada uma esta escrita em DESENVOLVIMENTO antes
de medir:

  2.1 Kafka com Nagle ligado x desligado, a 20 e 100 mil ev/s.
  2.2 RabbitMQ com mensagem persistente x transitoria, a 40 mil.
  2.3 RabbitMQ com prefetch 300 x sem limite, a 40 e 60 mil.
  2.4 Janela do produtor: Kafka 1 milhao x 100 mil a 200 mil; RabbitMQ 8 mil
      x 100 mil em voo a 60 mil.

Cada linha do CSV ganha as colunas "decisao" e "lado".

.EXAMPLE
./decisoes-fase2.ps1
./decisoes-fase2.ps1 -Seconds 20 -Reps 1   # ensaio curto
#>
param(
    [int]$Reps = 3,
    [int]$Seconds = 90,
    [int]$WarmupSeconds = 10,
    [double]$Hz = 100,
    [double]$WindowStart = 600,
    [string]$Out = 'results/fase2-runs.csv',
    [string]$ConsumerReport = 'results/fase2-consumer.csv',
    [string]$ProducerReport = 'results/fase2-producer.csv'
)

Import-Module (Join-Path $PSScriptRoot 'Pitwall.Runner.psm1') -Force

$root = Split-Path $PSScriptRoot -Parent
$outPath = Join-Path $root $Out
$commit = Get-GitCommit

Assert-NoDash

if ($commit -like '*-modificado') {
    Write-Warning "Codigo com alteracoes nao commitadas ($commit)."
}

# Cada comparacao: broker, cargas e os dois lados com os argumentos avulsos
# que vencem o perfil padrao (Invoke-PitwallRun -ProducerExtraArgs/-ConsumerExtraArgs).
$decisions = @(
    @{ Id = '2.1-nagle'; Broker = 'kafka'; Rates = @(20000, 100000); Sides = @(
        @{ Name = 'nagle-ligado';    Producer = @('--nagle', 'on');  Consumer = @('--nagle', 'on') },
        @{ Name = 'nagle-desligado'; Producer = @('--nagle', 'off'); Consumer = @('--nagle', 'off') }) },
    @{ Id = '2.4-janela-kafka'; Broker = 'kafka'; Rates = @(200000); Sides = @(
        @{ Name = 'fila-1000000'; Producer = @('--queue-max-messages', '1000000'); Consumer = @() },
        @{ Name = 'fila-100000';  Producer = @('--queue-max-messages', '100000');  Consumer = @() }) },
    @{ Id = '2.2-persistencia'; Broker = 'rabbit'; Rates = @(40000); Sides = @(
        @{ Name = 'persistente'; Producer = @();              Consumer = @() },
        @{ Name = 'transitoria'; Producer = @('--transient'); Consumer = @() }) },
    @{ Id = '2.3-prefetch'; Broker = 'rabbit'; Rates = @(40000, 60000); Sides = @(
        @{ Name = 'prefetch-300';        Producer = @(); Consumer = @('--prefetch', '300') },
        @{ Name = 'prefetch-sem-limite'; Producer = @(); Consumer = @('--prefetch', '0') }) },
    @{ Id = '2.4-janela-rabbit'; Broker = 'rabbit'; Rates = @(60000); Sides = @(
        @{ Name = 'em-voo-8000';   Producer = @('--confirm-batch', '1000');  Consumer = @() },
        @{ Name = 'em-voo-100000'; Producer = @('--confirm-batch', '12500'); Consumer = @() }) }
)

$total = ($decisions | ForEach-Object { $_.Rates.Count * $_.Sides.Count * $Reps } | Measure-Object -Sum).Sum
$index = 0
$failures = 0
$active = ''
$started = Get-Date

Write-Host "Fase 2: $total rodadas | $Seconds s medidos | $Hz Hz | commit $commit" -ForegroundColor Cyan

foreach ($d in $decisions) {
    if ($d.Broker -ne $active) {
        Write-Host "--- broker: $(Use-Broker -Broker $d.Broker -CpuMode cpuset)" -ForegroundColor Yellow
        $active = $d.Broker
    }

    foreach ($rate in $d.Rates) {
        for ($rep = 1; $rep -le $Reps; $rep++) {
            # Intercalado: os dois lados em cada repeticao, para que uma deriva
            # da maquina ao longo do tempo pese igual nos dois.
            foreach ($side in $d.Sides) {
                $index++
                $prefix = "[{0,2}/{1}]" -f $index, $total

                try {
                    $row = Invoke-PitwallRun -Broker $d.Broker -Mode direct -Rate $rate `
                        -Seconds $Seconds -WarmupSeconds $WarmupSeconds -Replication $rep -Persist `
                        -Hz $Hz -WindowStart $WindowStart -Profile padrao `
                        -ProducerExtraArgs $side.Producer -ConsumerExtraArgs $side.Consumer `
                        -ConsumerReport $ConsumerReport -ProducerReport $ProducerReport -Commit $commit
                }
                catch {
                    $row = $null
                    Write-Warning $_.Exception.Message
                }

                if ($null -eq $row) {
                    $failures++
                    Write-Host "$prefix $($d.Id) $($side.Name) @ $rate rep $rep`: FALHOU" -ForegroundColor Red
                    continue
                }

                $row | Add-Member -NotePropertyName decisao -NotePropertyValue $d.Id
                $row | Add-Member -NotePropertyName lado -NotePropertyValue $side.Name
                $row | Export-Csv -Path $outPath -Append -NoTypeInformation -Encoding UTF8

                Write-Host ("{0} {1,-18} {2,-20} @{3,7}  vazao={4,9:N0}  P99={5,7:N2} ms  atraso={6} ms" -f `
                    $prefix, $d.Id, $side.Name, $rate, [double]$row.throughput,
                    ([double]$row.p99_us / 1000), $row.producer_jitter_ms)
            }
        }
    }
}

$elapsed = (Get-Date) - $started
Write-Host "Concluido: $($total - $failures) de $total rodadas em $([int]$elapsed.TotalMinutes) min. Resultado: $outPath" -ForegroundColor Cyan
