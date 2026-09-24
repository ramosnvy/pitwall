<#
.SYNOPSIS
Carrega um CSV de rodadas (results/*-runs.csv) na tabela matrix_run do
PostgreSQL, para consulta em SQL e para o painel "Resultados da matriz".

.DESCRIPTION
Os CSVs continuam sendo a fonte da verdade: a carga apaga as linhas daquele
arquivo e as reinsere, entao pode ser repetida sem duplicar nada. Colunas que
um CSV mais antigo nao tem ficam nulas.

Executa antes infra/postgres/init/02-results.sql, que cria a tabela e a view
de validade se ainda nao existirem.

.EXAMPLE
./load-results.ps1 -In results/matrix-v2-runs.csv
#>
param(
    [string[]]$In = @('results/matrix-v2-runs.csv'),
    [string]$Container = 'pitwall-postgres'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$inv = [System.Globalization.CultureInfo]::InvariantCulture

# Coluna da tabela -> coluna do CSV.
$columns = [ordered]@{
    run_id                     = 'run_id'
    finished_at                = 'timestamp'
    architecture               = 'architecture'
    target_rate                = 'target_rate'
    replication                = 'replication'
    throughput                 = 'throughput'
    mean_us                    = 'mean_us'
    p50_us                     = 'p50_us'
    p95_us                     = 'p95_us'
    p99_us                     = 'p99_us'
    max_us                     = 'max_us'
    consumer_cpu_avg           = 'cpu_avg'
    consumer_container_cpu_avg = 'consumer_container_cpu_avg'
    consumer_mem_peak_mb       = 'mem_peak_mb'
    producer_cpu_avg           = 'producer_cpu_avg'
    broker_cpu_avg             = 'broker_cpu_avg'
    broker_mem_peak_mb         = 'broker_mem_peak_mb'
    db_cpu_avg                 = 'db_cpu_avg'
    windows_dropped            = 'windows_dropped'
    digest_hash                = 'digest_hash'
    producer_jitter_ms         = 'producer_jitter_ms'
    client_placement           = 'client_placement'
    code_commit                = 'commit'
    lane_hash                  = 'lane_hash'
    broker_cpu_mode            = 'broker_cpu_mode'
    broker_throttled_pct       = 'broker_throttled_pct'
    consumer_throttled_pct     = 'consumer_throttled_pct'
    producer_throttled_pct     = 'producer_throttled_pct'
    sample_hz                  = 'sample_hz'
    profile                    = 'profile'
    client_config              = 'client_config'
    producer_config            = 'producer_config'
    dotnet_env                 = 'dotnet_env'
    incomplete_records         = 'incomplete_records'
}

$text = @('run_id', 'architecture', 'digest_hash', 'client_placement', 'code_commit', 'finished_at',
          'lane_hash', 'broker_cpu_mode', 'sample_hz',
          'profile', 'client_config', 'producer_config', 'dotnet_env')

function Sql-Value($value, [bool]$isText) {
    if ([string]::IsNullOrWhiteSpace($value)) { return 'NULL' }
    if ($isText) { return "'" + ($value -replace "'", "''") + "'" }
    # Numeros sempre com ponto decimal; rejeita o que nao for numero.
    $n = 0.0
    if (-not [double]::TryParse($value, [System.Globalization.NumberStyles]::Float, $inv, [ref]$n)) { return 'NULL' }
    return $n.ToString('R', $inv)
}

$schema = Get-Content (Join-Path $root 'infra/postgres/init/02-results.sql') -Raw
$sql = [System.Text.StringBuilder]::new()
# Os NOTICEs do esquema idempotente ("ja existe, pulando") iriam para o stderr
# e o PowerShell os trataria como erro.
[void]$sql.AppendLine('SET client_min_messages TO WARNING;')
[void]$sql.AppendLine($schema)
[void]$sql.AppendLine('BEGIN;')

foreach ($file in $In) {
    $path = Join-Path $root $file
    $source = Split-Path $file -Leaf
    $rows = @(Import-Csv $path)
    $present = $rows[0].PSObject.Properties.Name

    [void]$sql.AppendLine("DELETE FROM matrix_run WHERE source = '$source';")

    foreach ($r in $rows) {
        $values = foreach ($col in $columns.Keys) {
            $csv = $columns[$col]
            $raw = if ($present -contains $csv) { $r.$csv } else { $null }
            Sql-Value $raw ($text -contains $col)
        }
        [void]$sql.AppendLine("INSERT INTO matrix_run (source, $($columns.Keys -join ', ')) VALUES ('$source', $($values -join ', '));")
    }

    Write-Host "$source : $($rows.Count) rodadas"
}

[void]$sql.AppendLine('COMMIT;')

$sql.ToString() | docker exec -i $Container psql -q -v ON_ERROR_STOP=1 -U pitwall -d pitwall
if ($LASTEXITCODE -ne 0) { throw "Falha ao carregar no PostgreSQL (exit $LASTEXITCODE)" }

docker exec $Container psql -U pitwall -d pitwall -c "SELECT source, count(*) AS rodadas, count(*) FILTER (WHERE valid) AS validas FROM v_matrix GROUP BY source ORDER BY source;"
