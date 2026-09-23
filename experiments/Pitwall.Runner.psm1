# Funcoes compartilhadas pelos scripts de experimento.
#
# Uma rodada e sempre: zerar o estado dos brokers, esperar o sistema assentar,
# subir o consumidor, publicar com o produtor, esperar o consumidor receber
# tudo e coletar as metricas dos containers no intervalo medido.
#
# O consumidor precisa comecar ANTES do produtor -- se os eventos ficarem
# parados no broker esperando alguem consumir, a latencia medida passa a
# incluir esse tempo de espera e nao mede mais a arquitetura.
#
# MODO CONTAINERIZADO (padrao). Produtor e consumidor rodam em containers na
# mesma rede Docker dos brokers, como o TCC1 declara ("toda a infraestrutura
# sera containerizada com Docker"). Rodando no Windows, o Kafka apresentava
# latencia bimodal -- 3 ms ou 25 ms por rodada, ao acaso -- que desaparece
# dentro da rede Docker: 0 de 10 rodadas lentas, contra ~43% no host. A causa
# esta no caminho Windows-VM (docs/REVISAO-TECNICA.md). O modo de processos
# no host fica disponivel com -HostProcesses, para reproduzir a comparacao.

$script:Docker = "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
$script:Dotnet = "$env:ProgramFiles\dotnet\dotnet.exe"
$script:Prometheus = 'http://localhost:9090'
$script:Invariant = [System.Globalization.CultureInfo]::InvariantCulture
$script:RuntimeImage = 'mcr.microsoft.com/dotnet/runtime:10.0'
$script:Network = 'pitwall_default'

function Invoke-KafkaTopics {
    param([string[]]$Arguments)

    & $script:Docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:19092 @Arguments 2>$null
}

function Reset-KafkaTopic {
    param([int]$Partitions = 4)

    # Topico recriado a cada rodada: sem isso, a rodada seguinte leria as
    # mensagens da anterior e o log cresceria sem limite.
    #
    # A exclusao no Kafka e assincrona. Recriar imediatamente pode falhar com
    # "topic marked for deletion" -- e, com a criacao automatica desligada, a
    # rodada seguinte publicaria num topico inexistente. Numa execucao
    # desatendida de horas, uma corrida dessas perderia rodadas em silencio.
    Invoke-KafkaTopics @('--delete', '--topic', 'telemetry') | Out-Null

    for ($i = 0; $i -lt 30; $i++) {
        $topics = Invoke-KafkaTopics @('--list')
        if (-not ($topics -contains 'telemetry')) { break }
        Start-Sleep -Milliseconds 500
    }

    for ($i = 0; $i -lt 10; $i++) {
        Invoke-KafkaTopics @('--create', '--topic', 'telemetry',
            '--partitions', "$Partitions", '--replication-factor', '1') | Out-Null

        $describe = Invoke-KafkaTopics @('--describe', '--topic', 'telemetry') | Out-String
        if ($describe -match "PartitionCount:\s*$Partitions\b") { return }
        Start-Sleep -Seconds 1
    }

    throw "Nao foi possivel recriar o topico telemetry com $Partitions particoes."
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

function Get-ContainerMetrics {
    <#
    .SYNOPSIS
    CPU e memoria de um container no intervalo medido, via Prometheus/cAdvisor.

    .DESCRIPTION
    O TCC1 exige CPU e memoria "abrangendo todos os modulos do sistema". No
    modo containerizado, produtor e consumidor tambem sao containers, e todos
    os modulos sao medidos pela mesma regua.
    #>
    param(
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][DateTimeOffset]$Start,
        [Parameter(Mandatory)][DateTimeOffset]$End
    )

    $result = [ordered]@{ cpu_avg = ''; cpu_peak = ''; mem_avg_mb = ''; mem_peak_mb = '' }

    if ($End -le $Start) { return $result }

    $s = $Start.ToUnixTimeSeconds()
    $e = $End.ToUnixTimeSeconds()

    # CPU em percentual de um nucleo (100 = um nucleo inteiro ocupado).
    $cpuQuery = "sum(rate(container_cpu_usage_seconds_total{name=`"$Container`"}[15s])) * 100"
    $memQuery = "sum(container_memory_working_set_bytes{name=`"$Container`"}) / 1048576"

    foreach ($pair in @(@('cpu', $cpuQuery), @('mem', $memQuery))) {
        $url = "$script:Prometheus/api/v1/query_range?query=$([uri]::EscapeDataString($pair[1]))&start=$s&end=$e&step=5"

        try {
            $response = Invoke-RestMethod -Uri $url -TimeoutSec 10
            $values = @()

            foreach ($series in $response.data.result) {
                foreach ($point in $series.values) {
                    $values += [double]::Parse($point[1], $script:Invariant)
                }
            }

            if ($values.Count -gt 0) {
                $stats = $values | Measure-Object -Average -Maximum
                $avg = $stats.Average.ToString('0.0', $script:Invariant)
                $max = $stats.Maximum.ToString('0.0', $script:Invariant)

                if ($pair[0] -eq 'cpu') { $result.cpu_avg = $avg; $result.cpu_peak = $max }
                else { $result.mem_avg_mb = $avg; $result.mem_peak_mb = $max }
            }
        }
        catch {
            Write-Warning "Prometheus indisponivel para $Container ($($pair[0])): $($_.Exception.Message)"
        }
    }

    return $result
}

function Get-GitCommit {
    $root = Split-Path $PSScriptRoot -Parent
    $git = "$env:ProgramFiles\Git\cmd\git.exe"
    $sha = (& $git -C $root rev-parse --short HEAD 2>$null)

    # So o codigo conta como "modificado". Os CSVs de resultado sao versionados
    # e crescem durante a propria execucao; olhar o repositorio inteiro
    # marcaria como modificado o codigo que produziu esses mesmos resultados.
    $dirty = (& $git -C $root status --porcelain -- src experiments tools 2>$null)

    if ($dirty) { return "$sha-modificado" }
    return $sha
}

function Wait-Container {
    param([string]$Name, [int]$TimeoutSeconds)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        $running = (& $script:Docker inspect -f '{{.State.Running}}' $Name 2>$null)
        if ($running -ne 'true') { return $true }
        Start-Sleep -Milliseconds 500
    }

    return $false
}

function Invoke-PitwallRun {
    <#
    .SYNOPSIS
    Executa uma rodada completa e devolve uma linha unica com produtor,
    consumidor e metricas dos containers.
    #>
    param(
        [Parameter(Mandatory)][string]$Broker,
        [Parameter(Mandatory)][string]$Mode,
        [Parameter(Mandatory)][int]$Rate,
        [Parameter(Mandatory)][int]$Seconds,
        [int]$WarmupSeconds = 10,
        [int]$Partitions = 4,
        [int]$Replication = 1,
        [switch]$Persist,
        [switch]$HostProcesses,
        [string]$Dataset = 'data/raw/9472/car_data.jsonl',
        [string]$ConsumerReport = 'results/consumer.csv',
        [string]$ProducerReport = 'results/producer.csv',
        [int]$IdleTimeout = 15,
        [int]$CooldownSeconds = 5,
        [double]$SyntheticCostUs = 0,
        [double]$ConsumerCpus = 3,
        [string]$ConsumerMemory = '1g',
        [double]$ProducerCpus = 2,
        [string]$ProducerMemory = '1g',
        [string]$Commit = ''
    )

    $root = Split-Path $PSScriptRoot -Parent
    $runId = [guid]::NewGuid().ToString()

    # O produtor publica aquecimento + medicao; o consumidor descarta o
    # aquecimento. A janela medida tem exatamente $Seconds de carga estavel.
    $events = [long]$Rate * ($Seconds + $WarmupSeconds)

    $logDir = Join-Path $env:TEMP 'pitwall-runs'
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
    $log = Join-Path $logDir "$Broker-$Mode-$Rate-$Replication.log"

    Reset-Broker -Broker $Broker -Partitions $Partitions

    # Assentamento entre rodadas: o broker ainda pode estar limpando o log ou
    # descarregando para disco o trabalho da rodada anterior.
    Start-Sleep -Seconds $CooldownSeconds

    $inv = $script:Invariant

    # Enderecos e caminhos dependem de onde os clientes rodam: no host, pelas
    # portas publicadas; em container, pelos nomes de servico da rede Docker.
    if ($HostProcesses) {
        $bootstrap = 'localhost:9092'
        $rabbitHost = 'localhost'
        $conn = 'Host=localhost;Port=5432;Username=pitwall;Password=pitwall;Database=pitwall'
        $datasetArg = Join-Path $root $Dataset
        $consumerReportArg = Join-Path $root $ConsumerReport
        $producerReportArg = Join-Path $root $ProducerReport
    }
    else {
        $bootstrap = 'kafka:19092'
        $rabbitHost = 'rabbitmq'
        $conn = 'Host=postgres;Port=5432;Username=pitwall;Password=pitwall;Database=pitwall'
        $datasetArg = '/data/' + (Split-Path $Dataset -Leaf)
        $consumerReportArg = '/results/' + (Split-Path $ConsumerReport -Leaf)
        $producerReportArg = '/results/' + (Split-Path $ProducerReport -Leaf)
    }

    $consumerArgs = @(
        '--broker', $Broker, '--mode', $Mode,
        '--partitions', $Partitions,
        '--idle-timeout', $IdleTimeout,
        '--warmup-seconds', $WarmupSeconds,
        '--expected-events', $events,
        '--target-rate', $Rate,
        '--replication', $Replication,
        '--synthetic-cost-us', $SyntheticCostUs.ToString($inv),
        '--run-id', $runId,
        '--timer-resolution-ms', '1',
        '--bootstrap', $bootstrap,
        '--rabbit-host', $rabbitHost,
        '--conn', $conn,
        '--report', $consumerReportArg
    )

    if ($Persist) { $consumerArgs += @('--persist', '--truncate') }

    $producerArgs = @(
        '--dataset', $datasetArg,
        '--rate', $Rate,
        '--events', $events,
        '--warmup', '0',
        '--duration', ($Seconds + $WarmupSeconds + 120),
        '--partitions', $Partitions,
        '--sink', $Broker,
        '--timer-resolution-ms', '1',
        '--bootstrap', $bootstrap,
        '--rabbit-host', $rabbitHost,
        '--report', $producerReportArg
    )

    $timeout = ($Seconds + $WarmupSeconds) * 3 + 180

    if ($HostProcesses) {
        $consumer = Start-Process -FilePath $script:Dotnet `
            -ArgumentList (@('run', '-c', 'Release', '--no-build', '--project', (Join-Path $root 'src/Pitwall.Consumer'), '--') + $consumerArgs) `
            -NoNewWindow -PassThru -RedirectStandardOutput $log -RedirectStandardError "$log.err"

        Start-Sleep -Seconds 3

        $producerStart = [DateTimeOffset]::UtcNow
        & $script:Dotnet (@('run', '-c', 'Release', '--no-build', '--project', (Join-Path $root 'src/Pitwall.Replayer'), '--') + $producerArgs) | Out-Null
        $producerEnd = [DateTimeOffset]::UtcNow

        $consumer | Wait-Process -Timeout $timeout

        if (-not $consumer.HasExited) {
            Write-Warning "Consumidor nao encerrou; matando processo ($Broker-$Mode-$Rate)"
            $consumer | Stop-Process -Force
            return $null
        }
    }
    else {
        & $script:Docker rm -f pitwall-consumer pitwall-producer 2>$null | Out-Null

        $resultsDir = (Join-Path $root 'results') -replace '\\', '/'
        $dataDir = (Split-Path (Join-Path $root $Dataset) -Parent) -replace '\\', '/'
        $consumerBin = (Join-Path $root 'src/Pitwall.Consumer/bin/Release/net10.0') -replace '\\', '/'
        $producerBin = (Join-Path $root 'src/Pitwall.Replayer/bin/Release/net10.0') -replace '\\', '/'

        # Limites fixos de CPU e memoria, como nos brokers: sem eles, o
        # consumidor de uma arquitetura poderia simplesmente usar mais
        # recursos que o de outra.
        $consumerDocker = @(
            'run', '-d', '--name', 'pitwall-consumer', '--network', $script:Network,
            '--cpus', $ConsumerCpus.ToString($inv), '--memory', $ConsumerMemory,
            '-v', "${consumerBin}:/app:ro", '-v', "${resultsDir}:/results",
            $script:RuntimeImage, 'dotnet', '/app/Pitwall.Consumer.dll'
        ) + $consumerArgs

        & $script:Docker $consumerDocker | Out-Null
        Start-Sleep -Seconds 3

        $producerDocker = @(
            'run', '--name', 'pitwall-producer', '--network', $script:Network,
            '--cpus', $ProducerCpus.ToString($inv), '--memory', $ProducerMemory,
            '-v', "${producerBin}:/app:ro", '-v', "${dataDir}:/data:ro", '-v', "${resultsDir}:/results",
            $script:RuntimeImage, 'dotnet', '/app/Pitwall.Replayer.dll'
        ) + $producerArgs

        $producerStart = [DateTimeOffset]::UtcNow
        & $script:Docker $producerDocker 2>&1 | Out-File "$log.producer" -Encoding utf8
        $producerEnd = [DateTimeOffset]::UtcNow

        $finished = Wait-Container -Name 'pitwall-consumer' -TimeoutSeconds $timeout
        & $script:Docker logs pitwall-consumer 2>&1 | Out-File $log -Encoding utf8

        if (-not $finished) {
            Write-Warning "Consumidor nao encerrou; removendo container ($Broker-$Mode-$Rate)"
        }
    }

    $consumerPath = Join-Path $root $ConsumerReport
    $row = $null

    if (Test-Path $consumerPath) {
        $row = @(Import-Csv $consumerPath) | Where-Object { $_.run_id -eq $runId } | Select-Object -Last 1
    }

    # Recursos na janela medida: do fim do aquecimento ao fim da publicacao. A
    # margem de 3 s cobre a carga do dataset, que antecede a publicacao.
    $measureStart = $producerStart.AddSeconds($WarmupSeconds + 3)
    $brokerContainer = 'pitwall-kafka'
    if ($Broker -ne 'kafka') { $brokerContainer = 'pitwall-rabbitmq' }

    $brokerMetrics = Get-ContainerMetrics -Container $brokerContainer -Start $measureStart -End $producerEnd
    $dbMetrics = Get-ContainerMetrics -Container 'pitwall-postgres' -Start $measureStart -End $producerEnd
    $consumerMetrics = $null
    $producerMetrics = $null

    if (-not $HostProcesses) {
        $consumerMetrics = Get-ContainerMetrics -Container 'pitwall-consumer' -Start $measureStart -End $producerEnd
        $producerMetrics = Get-ContainerMetrics -Container 'pitwall-producer' -Start $measureStart -End $producerEnd
        & $script:Docker rm -f pitwall-consumer pitwall-producer 2>$null | Out-Null
    }

    if ($null -eq $row) { return $null }

    # Lado do produtor. Sem ele nao da para distinguir "o consumidor nao
    # acompanhou" de "o produtor nunca conseguiu publicar a taxa alvo".
    $producerPath = Join-Path $root $ProducerReport
    $producerRate = ''
    $producerJitter = ''

    if (Test-Path $producerPath) {
        $p = @(Import-Csv $producerPath) | Select-Object -Last 1
        if ($null -ne $p) {
            $producerRate = $p.achieved_rate
            $producerJitter = $p.max_lateness_ms
        }
    }

    $enriched = [ordered]@{}
    foreach ($property in $row.PSObject.Properties) { $enriched[$property.Name] = $property.Value }

    $enriched['producer_achieved_rate'] = $producerRate
    $enriched['producer_jitter_ms'] = $producerJitter
    $enriched['broker_cpu_avg'] = $brokerMetrics.cpu_avg
    $enriched['broker_cpu_peak'] = $brokerMetrics.cpu_peak
    $enriched['broker_mem_avg_mb'] = $brokerMetrics.mem_avg_mb
    $enriched['broker_mem_peak_mb'] = $brokerMetrics.mem_peak_mb
    $enriched['db_cpu_avg'] = $dbMetrics.cpu_avg
    $enriched['db_cpu_peak'] = $dbMetrics.cpu_peak
    $enriched['db_mem_avg_mb'] = $dbMetrics.mem_avg_mb
    $enriched['db_mem_peak_mb'] = $dbMetrics.mem_peak_mb

    if ($null -ne $consumerMetrics) {
        $enriched['consumer_container_cpu_avg'] = $consumerMetrics.cpu_avg
        $enriched['consumer_container_mem_peak_mb'] = $consumerMetrics.mem_peak_mb
        $enriched['producer_cpu_avg'] = $producerMetrics.cpu_avg
        $enriched['producer_cpu_peak'] = $producerMetrics.cpu_peak
        $enriched['producer_mem_peak_mb'] = $producerMetrics.mem_peak_mb
    }

    $placement = 'container'
    if ($HostProcesses) { $placement = 'host' }

    $enriched['client_placement'] = $placement
    $enriched['warmup_seconds'] = $WarmupSeconds
    $enriched['measure_seconds'] = $Seconds
    $enriched['commit'] = $Commit

    return [pscustomobject]$enriched
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

    $throughput = [double]::Parse($Row.throughput, $script:Invariant)
    $p99Ms = [double]::Parse($Row.p99_us, $script:Invariant) / 1000.0

    $keptUp = $throughput -ge ($Rate * $ThroughputFloor)
    $latencyOk = $p99Ms -le $P99CeilingMs

    # O jitter do produtor e criterio de VALIDADE, nao so de saturacao: se o
    # gerador nao sustentou a taxa alvo, a rodada nao mede a arquitetura.
    $jitterOk = $true

    if ($Row.producer_jitter_ms) {
        $jitterOk = [double]::Parse($Row.producer_jitter_ms, $script:Invariant) -le $JitterCeilingMs
    }

    return -not ($keptUp -and $latencyOk -and $jitterOk)
}

Export-ModuleMember -Function Reset-KafkaTopic, Reset-RabbitQueues, Reset-Broker,
    Get-ContainerMetrics, Get-GitCommit, Invoke-PitwallRun, Test-RunSaturated
