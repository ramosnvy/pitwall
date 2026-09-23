# Funcoes compartilhadas pelos scripts de experimento.
#
# Uma rodada e sempre: zerar o estado dos brokers e do banco, subir o
# consumidor, publicar com o produtor, esperar o consumidor drenar. O
# consumidor precisa comecar ANTES do produtor -- se os eventos ficarem
# parados no broker esperando alguem consumir, a latencia medida passa a
# incluir esse tempo de espera e nao mede mais a arquitetura.

$script:Docker = "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
$script:Dotnet = "$env:ProgramFiles\dotnet\dotnet.exe"

function Reset-KafkaTopic {
    param([int]$Partitions = 4)

    # Topico recriado a cada rodada: sem isso, a rodada seguinte leria as
    # mensagens da anterior e o log cresceria sem limite (a 100 mil ev/s sao
    # cerca de 7 MB/s).
    & $script:Docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:19092 --delete --topic telemetry 2>$null | Out-Null

    & $script:Docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:19092 --create --topic telemetry `
        --partitions $Partitions --replication-factor 1 2>$null | Out-Null
}

function Reset-RabbitQueues {
    param([int]$Partitions = 4)

    for ($lane = 0; $lane -lt $Partitions; $lane++) {
        & $script:Docker exec pitwall-rabbitmq rabbitmqctl purge_queue "telemetry.$lane" 2>$null | Out-Null
    }
}

function Reset-Broker {
    param([string]$Broker, [int]$Partitions = 4)

    if ($Broker -eq 'kafka') { Reset-KafkaTopic -Partitions $Partitions }
    else { Reset-RabbitQueues -Partitions $Partitions }
}

function Invoke-PitwallRun {
    <#
    .SYNOPSIS
    Executa uma rodada completa e devolve a linha de resultado do consumidor.
    #>
    param(
        [Parameter(Mandatory)][string]$Broker,
        [Parameter(Mandatory)][string]$Mode,
        [Parameter(Mandatory)][int]$Rate,
        [Parameter(Mandatory)][int]$Seconds,
        [int]$Partitions = 4,
        [int]$Replication = 1,
        [switch]$Persist,
        [string]$Dataset = 'data/raw/9472/car_data.jsonl',
        [string]$ConsumerReport = 'results/consumer.csv',
        [string]$ProducerReport = 'results/producer.csv',
        [int]$IdleTimeout = 6,
        [double]$SyntheticCostUs = 0
    )

    $root = Split-Path $PSScriptRoot -Parent
    $events = $Rate * $Seconds
    $logDir = Join-Path $env:TEMP 'pitwall-runs'
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
    $log = Join-Path $logDir "$Broker-$Mode-$Rate-$Replication.log"

    Reset-Broker -Broker $Broker -Partitions $Partitions

    $consumerArgs = @(
        'run', '-c', 'Release', '--no-build',
        '--project', (Join-Path $root 'src/Pitwall.Consumer'),
        '--',
        '--broker', $Broker, '--mode', $Mode,
        '--partitions', $Partitions,
        '--idle-timeout', $IdleTimeout,
        '--target-rate', $Rate,
        '--replication', $Replication,
        '--synthetic-cost-us', $SyntheticCostUs,
        '--report', (Join-Path $root $ConsumerReport)
    )

    if ($Persist) { $consumerArgs += @('--persist', '--truncate') }

    $consumer = Start-Process -FilePath $script:Dotnet -ArgumentList $consumerArgs `
        -NoNewWindow -PassThru -RedirectStandardOutput $log -RedirectStandardError "$log.err"

    # O consumidor precisa estar assinando antes de o produtor publicar.
    Start-Sleep -Seconds 3

    $producerArgs = @(
        'run', '-c', 'Release', '--no-build',
        '--project', (Join-Path $root 'src/Pitwall.Replayer'),
        '--',
        '--dataset', (Join-Path $root $Dataset),
        '--rate', $Rate,
        '--events', $events,
        '--warmup', '0',
        '--duration', ($Seconds + 120),
        '--partitions', $Partitions,
        '--sink', $Broker,
        '--report', (Join-Path $root $ProducerReport)
    )

    & $script:Dotnet $producerArgs | Out-Null

    # Espera o consumidor drenar e encerrar sozinho pelo tempo ocioso.
    $consumer | Wait-Process -Timeout (($Seconds * 3) + 180)

    if (-not $consumer.HasExited) {
        Write-Warning "Consumidor nao encerrou; matando processo ($Broker-$Mode-$Rate)"
        $consumer | Stop-Process -Force
        return $null
    }

    $rows = @(Import-Csv (Join-Path $root $ConsumerReport))
    if ($rows.Count -eq 0) { return $null }

    $row = $rows[-1]

    # Anexa o lado do produtor. Sem ele nao da para distinguir "o consumidor
    # nao acompanhou" de "o produtor nunca conseguiu publicar a taxa alvo" --
    # a confusao que o piloto expos no RabbitMQ.
    $producerPath = Join-Path $root $ProducerReport

    if (Test-Path $producerPath) {
        $producerRows = @(Import-Csv $producerPath)

        if ($producerRows.Count -gt 0) {
            $p = $producerRows[-1]
            $row | Add-Member -NotePropertyName producer_achieved_rate -NotePropertyValue $p.achieved_rate -Force
            $row | Add-Member -NotePropertyName producer_jitter_ms -NotePropertyValue $p.max_lateness_ms -Force
        }
    }

    return $row
}

function Test-RunSaturated {
    <#
    .SYNOPSIS
    Decide se a rodada saturou. Saturacao e a carga em que o sistema deixa de
    acompanhar o produtor -- e o ponto que o experimento procura.
    #>
    param(
        [Parameter(Mandatory)]$Row,
        [Parameter(Mandatory)][int]$Rate,
        [double]$ThroughputFloor = 0.90,
        [double]$P99CeilingMs = 1000,
        [double]$JitterCeilingMs = 50
    )

    if ($null -eq $Row) { return $true }

    $throughput = [double]$Row.throughput
    $p99Ms = [double]$Row.p99_us / 1000.0

    $keptUp = $throughput -ge ($Rate * $ThroughputFloor)
    $latencyOk = $p99Ms -le $P99CeilingMs

    # O jitter do produtor e criterio de VALIDADE, nao so de saturacao: se o
    # gerador nao sustentou a taxa alvo, a rodada nao mede a arquitetura.
    $jitterOk = $true

    if ($null -ne $Row.producer_jitter_ms) {
        $jitterOk = ([double]$Row.producer_jitter_ms) -le $JitterCeilingMs
    }

    return -not ($keptUp -and $latencyOk -and $jitterOk)
}

Export-ModuleMember -Function Reset-KafkaTopic, Reset-RabbitQueues, Reset-Broker,
    Invoke-PitwallRun, Test-RunSaturated
