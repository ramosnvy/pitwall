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

function Get-ThrottledPercent {
    <#
    .SYNOPSIS
    Fracao dos periodos CFS em que o container foi estrangulado pela cota de
    CPU, na janela medida. Ver docs/IMPLEMENTACAO.md, secao 2.
    #>
    param(
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][DateTimeOffset]$Start,
        [Parameter(Mandatory)][DateTimeOffset]$End
    )

    $seconds = [int][math]::Round(($End - $Start).TotalSeconds)
    if ($seconds -le 0) { return '' }

    $values = @{}
    foreach ($metric in @('container_cpu_cfs_throttled_periods_total', 'container_cpu_cfs_periods_total')) {
        $query = "sum(increase($metric{name=`"$Container`"}[${seconds}s]))"
        $url = "$script:Prometheus/api/v1/query?query=$([uri]::EscapeDataString($query))&time=$($End.ToUnixTimeSeconds())"
        try {
            $result = (Invoke-RestMethod -Uri $url -TimeoutSec 10).data.result
            $values[$metric] = if ($result) { [double]::Parse($result[0].value[1], $script:Invariant) } else { 0.0 }
        }
        catch {
            Write-Warning "Prometheus indisponivel para o estrangulamento de ${Container}: $($_.Exception.Message)"
            return ''
        }
    }

    # Sem periodos contados, o container nao tem cota (cpuset puro): nada a
    # estrangular.
    $periods = $values['container_cpu_cfs_periods_total']
    if ($periods -le 0) { return '0.00' }

    return (100 * $values['container_cpu_cfs_throttled_periods_total'] / $periods).ToString('0.00', $script:Invariant)
}

function Get-CpuMode {
    <#
    .SYNOPSIS
    Como a CPU de um container esta limitada: cota (--cpus), nucleos fixos
    (--cpuset-cpus) ou os dois. Lido do proprio container, para que o CSV
    registre o estado real e nao o pretendido.
    #>
    param([Parameter(Mandatory)][string]$Container)

    # Le o cgroup de dentro do container. O `docker inspect` nao serve: depois
    # de um `docker update --cpu-quota=-1`, ele continua mostrando NanoCpus
    # antigo, embora a cota ja tenha saido do kernel.
    $info = & $script:Docker exec $Container sh -c 'cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/cpuset.cpus.effective' 2>$null
    if (-not $info -or $info.Count -lt 2) { return '' }

    $quota, $period = ($info[0].Trim() -split '\s+')
    $parts = @()

    if ($quota -ne 'max') {
        $parts += 'quota:' + ([double]$quota / [double]$period).ToString('0.##', $script:Invariant)
    }
    $parts += 'cpus:' + $info[1].Trim()

    return $parts -join '+'
}

function Set-BrokerCpuMode {
    <#
    .SYNOPSIS
    Troca, com o container em execucao, o limite de CPU do broker entre cota
    (--cpus) e nucleos fixos sem cota (--cpuset-cpus). O A/B da
    docs/IMPLEMENTACAO.md, secao 2, depende disso.
    #>
    param(
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][ValidateSet('quota', 'cpuset')][string]$Mode,
        [double]$Cpus = 4,
        [string]$Cpuset = '4-7',
        [string]$AllCpus = '0-11'
    )

    if ($Mode -eq 'quota') {
        & $script:Docker update --cpus $Cpus.ToString($script:Invariant) --cpuset-cpus $AllCpus $Container | Out-Null
    }
    else {
        # --cpus 0 nao remove a cota; --cpu-quota=-1 remove.
        & $script:Docker update --cpu-quota=-1 --cpuset-cpus $Cpuset $Container | Out-Null
    }

    return Get-CpuMode -Container $Container
}

# Nucleos da VM do Docker (12) repartidos entre os componentes, sem cota e
# sem vizinhos. Mesmas capacidades do protocolo antigo: produtor 4, broker 4,
# consumidor 3; banco e coleta de metricas dividem o ultimo nucleo (o banco
# usou no maximo 17% de um nucleo na matriz, e a 100 Hz grava 27x menos).
$script:Layout = [ordered]@{ producer = '0-3'; broker = '4-7'; consumer = '8-10'; infra = '11'; all = '0-11' }
$script:Infra = @('pitwall-postgres', 'pitwall-prometheus', 'pitwall-cadvisor')
$script:Dash = @('pitwall-grafana', 'pitwall-loki', 'pitwall-alloy', 'pitwall-replay')

function Get-ClientCpuArgs {
    param([string]$CpuMode, [double]$Cpus, [string]$Cpuset)
    if ($CpuMode -eq 'cpuset') { return @('--cpuset-cpus', $Cpuset) }
    return @('--cpus', $Cpus.ToString($script:Invariant))
}

function Wait-Healthy {
    param([string]$Container, [int]$TimeoutSeconds = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = & $script:Docker inspect $Container --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' 2>$null
        if ($status -in @('healthy', 'running') -and $status -ne 'starting') { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

function Use-Broker {
    <#
    .SYNOPSIS
    Deixa no ar so o broker que vai ser medido, com o layout de nucleos do
    protocolo. O outro broker e parado: com nucleos exclusivos, os dois
    dividiriam os nucleos do broker, e mesmo ocioso o outro rouba ciclos. A
    matriz 7e283c2 rodou com os dois no ar (docs/AMEACAS-VALIDADE.md).
    #>
    param(
        [Parameter(Mandatory)][string]$Broker,
        [ValidateSet('cpuset', 'quota')][string]$CpuMode = 'cpuset'
    )

    $target = if ($Broker -eq 'kafka') { 'pitwall-kafka' } else { 'pitwall-rabbitmq' }
    $other = if ($Broker -eq 'kafka') { 'pitwall-rabbitmq' } else { 'pitwall-kafka' }

    & $script:Docker stop $other 2>$null | Out-Null
    & $script:Docker start $target 2>$null | Out-Null
    if (-not (Wait-Healthy -Container $target)) { throw "O broker $target nao ficou saudavel." }

    if ($CpuMode -eq 'cpuset') {
        $brokerMode = Set-BrokerCpuMode -Container $target -Mode cpuset -Cpuset $script:Layout.broker
        foreach ($c in $script:Infra) {
            & $script:Docker update --cpuset-cpus $script:Layout.infra $c | Out-Null
        }
        # O banco perde a cota: com um nucleo exclusivo, ela so poderia estrangula-lo.
        & $script:Docker update --cpu-quota=-1 pitwall-postgres | Out-Null
    }
    else {
        $brokerMode = Set-BrokerCpuMode -Container $target -Mode quota -AllCpus $script:Layout.all
        foreach ($c in $script:Infra) {
            & $script:Docker update --cpuset-cpus $script:Layout.all $c | Out-Null
        }
        & $script:Docker update --cpus 2 pitwall-postgres | Out-Null
    }

    return "$target $brokerMode"
}

function Assert-NoDash {
    # O painel disputa CPU com o que e medido; nas rodadas oficiais fica
    # desligado (docs/AMEACAS-VALIDADE.md).
    $running = @(& $script:Docker ps --format '{{.Names}}')
    $up = @($running | Where-Object { $_ -in $script:Dash })
    if ($up.Count -gt 0) {
        throw "Painel no ar durante a medicao: $($up -join ', '). Pare com: docker stop $($up -join ' ')"
    }
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

function Get-ProfileArgs {
    <#
    .SYNOPSIS
    Argumentos dos clientes para cada perfil de configuracao
    (docs/AUDITORIA-CONFIG.md e docs/DESENVOLVIMENTO.md, fase 1.5).

    padrao: os valores de fabrica das bibliotecas e dos brokers, mais as
    escolhas de equivalencia da auditoria. Os clientes ja nascem no padrao de
    fabrica; aqui entram so as escolhas da aplicacao. A janela do produtor
    RabbitMQ segura o mesmo total que a fila padrao do produtor Kafka: 12.500
    por lote x 2 lotes x 4 faixas = 100 mil. Provisorio ate as medicoes da
    fase 2.

    legado: a configuracao dos clientes usada ate 24/09 (matriz 7e283c2, 2x2
    e varredura a 100 Hz). Reproduz so o lado dos clientes: heap do Kafka,
    compressao do broker e limite de memoria do RabbitMQ mudaram no compose.

    ajustado: definido na fase 7, depois de medir o padrao.
    #>
    param(
        [Parameter(Mandatory)][string]$Profile,
        [Parameter(Mandatory)][string]$Broker
    )

    $kafka = $Broker -eq 'kafka'

    switch ($Profile) {
        'padrao' {
            if ($kafka) { return @{ Producer = @(); Consumer = @() } }
            return @{ Producer = @('--confirm-batch', '12500'); Consumer = @('--prefetch', '0') }
        }
        'legado' {
            if ($kafka) {
                return @{
                    Producer = @('--linger-ms', '5', '--batch-size', '65536', '--queue-max-messages', '1000000', '--nagle', 'on')
                    Consumer = @('--fetch-wait-max-ms', '10', '--nagle', 'on')
                }
            }
            return @{ Producer = @('--confirm-batch', '1000'); Consumer = @('--prefetch', '300') }
        }
        'ajustado' { throw 'O perfil ajustado so e definido na fase 7 (docs/DESENVOLVIMENTO.md).' }
        default { throw "Perfil desconhecido: $Profile. Use padrao ou legado." }
    }
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
        [double]$ProducerCpus = 4,
        [string]$ProducerMemory = '1g',
        # cpuset: nucleos exclusivos por container, sem cota (docs/IMPLEMENTACAO.md,
        # secao 7); quota: --cpus, como na matriz 7e283c2.
        [ValidateSet('cpuset', 'quota')][string]$CpuMode = 'cpuset',
        # Faixa de cada carro no RabbitMQ: crc32 (igual ao Kafka) ou modulo
        # (como na matriz 7e283c2). No Kafka quem decide e a librdkafka.
        [ValidateSet('crc32', 'modulo')][string]$LaneHash = 'crc32',
        # Frequencia por carro. 0 = cadencia nativa da OpenF1 (3,7 Hz); acima
        # disso o produtor reamostra por interpolacao (TelemetryInterpolator).
        [double]$Hz = 0,
        # Inicio do trecho da corrida usado, em segundos; negativo = corrida
        # inteira, como na matriz 7e283c2. Obrigatorio com -Hz.
        [double]$WindowStart = -1,
        # Perfil de configuracao dos clientes (Get-ProfileArgs).
        [ValidateSet('padrao', 'legado', 'ajustado')][string]$Profile = 'padrao',
        # Argumentos avulsos, para as medicoes de decisao da fase 2. Vencem os
        # do perfil: os clientes leem a primeira ocorrencia de cada opcao.
        [string[]]$ProducerExtraArgs = @(),
        [string[]]$ConsumerExtraArgs = @(),
        [string]$Commit = ''
    )

    $root = Split-Path $PSScriptRoot -Parent
    $runId = [guid]::NewGuid().ToString()
    $profileArgs = Get-ProfileArgs -Profile $Profile -Broker $Broker

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
        '--run-id', $runId,
        '--timer-resolution-ms', '1',
        '--bootstrap', $bootstrap,
        '--rabbit-host', $rabbitHost,
        '--lane-hash', $LaneHash,
        '--report', $producerReportArg
    )

    if ($Hz -gt 0 -and $WindowStart -lt 0) {
        throw 'Com -Hz, informe -WindowStart: a corrida inteira reamostrada nao cabe na memoria do produtor.'
    }
    if ($Hz -gt 0) { $producerArgs += @('--hz', $Hz.ToString($inv)) }
    if ($WindowStart -ge 0) { $producerArgs += @('--window-start', $WindowStart.ToString($inv)) }

    # Avulsos antes do perfil, para vencerem (primeira ocorrencia).
    $producerArgs = $producerArgs + $ProducerExtraArgs + $profileArgs.Producer
    $consumerArgs = $consumerArgs + $ConsumerExtraArgs + $profileArgs.Consumer

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

        # Rotulos da rodada: o painel local (profile "dash") filtra os logs por
        # eles. Nao alteram nada no que o container executa.
        $labels = @(
            '--label', "pitwall.run_id=$runId",
            '--label', "pitwall.architecture=$Broker-$Mode",
            '--label', "pitwall.rate=$Rate",
            '--label', "pitwall.replication=$Replication"
        )

        # Limites fixos de CPU e memoria, como nos brokers: sem eles, o
        # consumidor de uma arquitetura poderia simplesmente usar mais
        # recursos que o de outra. Com -CpuMode cpuset (padrao desde o 2x2),
        # cada cliente tem nucleos exclusivos e nenhuma cota; com quota, o
        # protocolo da matriz 7e283c2.
        $consumerDocker = @('run', '-d', '--name', 'pitwall-consumer', '--network', $script:Network,
                '--label', 'pitwall.role=consumer') +
            $labels +
            (Get-ClientCpuArgs -CpuMode $CpuMode -Cpus $ConsumerCpus -Cpuset $script:Layout.consumer) +
            @('--memory', $ConsumerMemory,
                '-v', "${consumerBin}:/app:ro", '-v', "${resultsDir}:/results",
                $script:RuntimeImage, 'dotnet', '/app/Pitwall.Consumer.dll') +
            $consumerArgs

        & $script:Docker $consumerDocker | Out-Null
        Start-Sleep -Seconds 3

        $producerDocker = @('run', '--name', 'pitwall-producer', '--network', $script:Network,
                '--label', 'pitwall.role=producer') +
            $labels +
            (Get-ClientCpuArgs -CpuMode $CpuMode -Cpus $ProducerCpus -Cpuset $script:Layout.producer) +
            @('--memory', $ProducerMemory,
                '-v', "${producerBin}:/app:ro", '-v', "${dataDir}:/data:ro", '-v', "${resultsDir}:/results",
                $script:RuntimeImage, 'dotnet', '/app/Pitwall.Replayer.dll') +
            $producerArgs

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
    $brokerThrottled = Get-ThrottledPercent -Container $brokerContainer -Start $measureStart -End $producerEnd
    $brokerCpuMode = Get-CpuMode -Container $brokerContainer
    $consumerMetrics = $null
    $producerMetrics = $null
    $consumerThrottled = ''
    $producerThrottled = ''

    if (-not $HostProcesses) {
        $consumerMetrics = Get-ContainerMetrics -Container 'pitwall-consumer' -Start $measureStart -End $producerEnd
        $producerMetrics = Get-ContainerMetrics -Container 'pitwall-producer' -Start $measureStart -End $producerEnd
        $consumerThrottled = Get-ThrottledPercent -Container 'pitwall-consumer' -Start $measureStart -End $producerEnd
        $producerThrottled = Get-ThrottledPercent -Container 'pitwall-producer' -Start $measureStart -End $producerEnd
        & $script:Docker rm -f pitwall-consumer pitwall-producer 2>$null | Out-Null
    }

    if ($null -eq $row) { return $null }

    # Lado do produtor. Sem ele nao da para distinguir "o consumidor nao
    # acompanhou" de "o produtor nunca conseguiu publicar a taxa alvo".
    $producerPath = Join-Path $root $ProducerReport
    $producerRate = ''
    $producerJitter = ''
    $producerConfig = ''
    $producerDotnetEnv = ''
    $producerQueueFullWaits = ''

    # Casamento pela chave da rodada, nunca pela posicao no arquivo. Na matriz
    # v2, um produtor caiu sem gravar relatorio, e pegar a ultima linha atribuiu
    # a rodada o jitter da rodada ANTERIOR -- ela pareceu valida, e so o digest
    # a barrou. Sem linha do produtor, os campos ficam vazios e a analise trata
    # a rodada como invalida.
    if (Test-Path $producerPath) {
        $p = @(Import-Csv $producerPath) | Where-Object { $_.run_id -eq $runId } | Select-Object -Last 1
        if ($null -ne $p) {
            $producerRate = $p.achieved_rate
            $producerJitter = $p.max_lateness_ms
            # Colunas de 24/09 em diante; vazias em relatorios mais antigos.
            $producerConfig = $p.client_config
            $producerDotnetEnv = $p.dotnet_env
            $producerQueueFullWaits = $p.queue_full_waits
        }
    }

    $enriched = [ordered]@{}
    foreach ($property in $row.PSObject.Properties) { $enriched[$property.Name] = $property.Value }

    $enriched['producer_achieved_rate'] = $producerRate
    $enriched['producer_jitter_ms'] = $producerJitter
    $enriched['producer_config'] = $producerConfig
    $enriched['producer_dotnet_env'] = $producerDotnetEnv
    $enriched['producer_queue_full_waits'] = $producerQueueFullWaits
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

    # Assimetrias da matriz 7e283c2 (docs/IMPLEMENTACAO.md): como os carros
    # sao divididos entre as faixas, como a CPU do broker e limitada, e quanto
    # cada container foi estrangulado pela cota na janela medida.
    $enriched['lane_hash'] = if ($Broker -eq 'kafka') { 'crc32' } else { $LaneHash }
    $enriched['sample_hz'] = if ($Hz -gt 0) { $Hz.ToString($script:Invariant) } else { 'nativo' }
    $enriched['cpu_protocol'] = if ($CpuMode -eq 'cpuset') {
        "cpuset produtor $($script:Layout.producer), broker $($script:Layout.broker), consumidor $($script:Layout.consumer), infra $($script:Layout.infra)"
    } else { 'quota' }
    $enriched['window_start_s'] = if ($WindowStart -ge 0) { $WindowStart.ToString($script:Invariant) } else { '' }
    $enriched['broker_cpu_mode'] = $brokerCpuMode
    $enriched['broker_throttled_pct'] = $brokerThrottled
    $enriched['consumer_throttled_pct'] = $consumerThrottled
    $enriched['producer_throttled_pct'] = $producerThrottled

    $enriched['client_placement'] = $placement
    $enriched['profile'] = $Profile
    $enriched['extra_args'] = (@($ProducerExtraArgs) + @($ConsumerExtraArgs)) -join ' '
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
    Get-ContainerMetrics, Get-GitCommit, Invoke-PitwallRun, Test-RunSaturated,
    Get-ThrottledPercent, Get-CpuMode, Set-BrokerCpuMode, Use-Broker, Assert-NoDash
