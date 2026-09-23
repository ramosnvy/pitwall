-- Esquema de destino do modulo de persistencia.
--
-- As tabelas sao UNLOGGED de proposito: o objetivo do experimento e medir o
-- custo de escrita do pipeline, nao a durabilidade do banco apos uma queda.
-- Isso reduz o trafego de WAL e evita que o banco vire o gargalo e mascare a
-- diferenca entre as arquiteturas (ver docs/PLANO.md, secao 1e).

CREATE UNLOGGED TABLE processed_event (
    run_id          UUID        NOT NULL,
    architecture    TEXT        NOT NULL,   -- ex.: kafka-channels
    driver_number   SMALLINT    NOT NULL,
    session_key     INTEGER     NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,   -- timestamp original da telemetria
    published_at    TIMESTAMPTZ NOT NULL,   -- momento da publicacao no broker
    processed_at    TIMESTAMPTZ NOT NULL,   -- fim do processamento
    speed           SMALLINT,
    rpm             INTEGER,
    n_gear          SMALLINT,
    throttle        SMALLINT,
    brake           SMALLINT,
    drs             SMALLINT
);

-- Resultado das agregacoes em janela produzidas pelo modulo de processamento.
CREATE UNLOGGED TABLE driver_window_stats (
    run_id          UUID        NOT NULL,
    architecture    TEXT        NOT NULL,
    driver_number   SMALLINT    NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    event_count     INTEGER     NOT NULL,
    avg_speed       REAL        NOT NULL,
    max_speed       SMALLINT    NOT NULL,
    hard_brakings   INTEGER     NOT NULL,
    gear_changes    INTEGER     NOT NULL,
    PRIMARY KEY (run_id, driver_number, window_start)
);

-- Uma linha por execucao do experimento: guarda o ambiente e os parametros
-- para que cada numero do artigo possa ser rastreado ate a rodada que o gerou.
CREATE TABLE IF NOT EXISTS experiment_run (
    run_id          UUID        PRIMARY KEY,
    architecture    TEXT        NOT NULL,
    broker          TEXT        NOT NULL,   -- kafka | rabbitmq
    mechanism       TEXT        NOT NULL,   -- direct | channels | pipelines
    target_rate     INTEGER     NOT NULL,   -- eventos/s alvo
    replication     SMALLINT    NOT NULL,   -- numero da repeticao
    persistence_on  BOOLEAN     NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    config          JSONB       NOT NULL,   -- parametros do broker e do app
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS ix_processed_event_run ON processed_event (run_id);
CREATE INDEX IF NOT EXISTS ix_experiment_run_arch ON experiment_run (architecture, target_rate);
