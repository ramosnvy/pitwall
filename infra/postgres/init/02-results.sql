-- Resultados consolidados das matrizes, para consulta e para o painel local.
--
-- Os CSVs de results/ continuam sendo a fonte da verdade; esta tabela e uma
-- copia carregada por analysis/load-results.ps1, que a recria por arquivo.
-- Idempotente: o carregador executa este arquivo antes de cada carga, entao
-- vale tambem para bancos criados antes dele existir.

CREATE TABLE IF NOT EXISTS matrix_run (
    source                      TEXT        NOT NULL,   -- arquivo de origem
    run_id                      TEXT        NOT NULL,
    finished_at                 TIMESTAMPTZ,
    architecture                TEXT        NOT NULL,
    target_rate                 INTEGER     NOT NULL,
    replication                 INTEGER,
    throughput                  DOUBLE PRECISION,
    mean_us                     DOUBLE PRECISION,
    p50_us                      DOUBLE PRECISION,
    p95_us                      DOUBLE PRECISION,
    p99_us                      DOUBLE PRECISION,
    max_us                      DOUBLE PRECISION,
    consumer_cpu_avg            DOUBLE PRECISION,   -- processo, medido por dentro
    consumer_container_cpu_avg  DOUBLE PRECISION,   -- container, pelo cAdvisor
    consumer_mem_peak_mb        DOUBLE PRECISION,
    producer_cpu_avg            DOUBLE PRECISION,
    broker_cpu_avg              DOUBLE PRECISION,
    broker_mem_peak_mb          DOUBLE PRECISION,
    db_cpu_avg                  DOUBLE PRECISION,
    windows_dropped             BIGINT,
    digest_hash                 TEXT,
    producer_jitter_ms          DOUBLE PRECISION,
    client_placement            TEXT,
    code_commit                 TEXT,
    PRIMARY KEY (source, run_id)
);

-- Mesmas regras de validade de analysis/summarize.ps1: sem relatorio do
-- produtor, jitter acima de 50 ms ou digest diferente do da maioria na mesma
-- carga, a rodada nao entra na analise.
CREATE OR REPLACE VIEW v_matrix AS
WITH digest_count AS (
    SELECT source, target_rate, digest_hash, count(*) AS n
    FROM matrix_run
    GROUP BY source, target_rate, digest_hash
), reference AS (
    SELECT DISTINCT ON (source, target_rate) source, target_rate, digest_hash
    FROM digest_count
    ORDER BY source, target_rate, n DESC
)
SELECT
    m.*,
    split_part(m.architecture, '-', 1) AS broker,
    split_part(m.architecture, '-', 2) AS mechanism,
    m.p50_us / 1000 AS p50_ms,
    m.p99_us / 1000 AS p99_ms,
    coalesce(m.consumer_container_cpu_avg, 0) + coalesce(m.producer_cpu_avg, 0)
        + coalesce(m.broker_cpu_avg, 0) + coalesce(m.db_cpu_avg, 0) AS total_cpu,
    concat_ws('+',
        CASE WHEN m.producer_jitter_ms IS NULL THEN 'produtor'
             WHEN m.producer_jitter_ms > 50 THEN 'jitter' END,
        CASE WHEN m.digest_hash IS DISTINCT FROM r.digest_hash THEN 'digest' END
    ) AS invalid_reason,
    (m.producer_jitter_ms IS NOT NULL AND m.producer_jitter_ms <= 50
        AND m.digest_hash = r.digest_hash) AS valid
FROM matrix_run m
JOIN reference r ON r.source = m.source AND r.target_rate = m.target_rate;
