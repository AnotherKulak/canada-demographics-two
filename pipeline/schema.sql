-- Canada Demographics DuckDB Schema
-- All counts are integers. Percentages are computed at query time.
-- IRCC data is suppressed for counts 0-5 (published as NULL here).

-- ─────────────────────────────────────────────────────────────────────────────
-- POPULATION
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS population_quarterly (
    ref_date        DATE        NOT NULL,
    geo             VARCHAR     NOT NULL,
    population      BIGINT      NOT NULL,
    source_table    VARCHAR     NOT NULL DEFAULT '17-10-0009-01',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_date, geo)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- STATUS BREAKDOWN (Census snapshots)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS status_breakdown_census (
    census_year     SMALLINT    NOT NULL,
    geo             VARCHAR     NOT NULL,
    status          VARCHAR     NOT NULL,
    count           BIGINT,
    source_table    VARCHAR     NOT NULL,
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (census_year, geo, status)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- NON-PERMANENT RESIDENTS BY TYPE (Quarterly, continuous)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS npr_by_type_quarterly (
    ref_date        DATE        NOT NULL,
    geo             VARCHAR     NOT NULL,
    npr_type        VARCHAR     NOT NULL,
    count           BIGINT,
    source_table    VARCHAR     NOT NULL DEFAULT '17-10-0121-01',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_date, geo, npr_type)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- STUDY PERMITS (Monthly IRCC)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS study_permits_monthly (
    ref_year        SMALLINT    NOT NULL,
    ref_month       SMALLINT,
    country         VARCHAR     NOT NULL,
    study_level     VARCHAR     NOT NULL DEFAULT '_',
    province        VARCHAR     NOT NULL DEFAULT '_',
    count           INTEGER,
    source_dataset  VARCHAR     NOT NULL DEFAULT 'IRCC-90115b00',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_year, ref_month, country, study_level, province)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- WORK PERMITS (Monthly IRCC)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS work_permits_monthly (
    ref_year        SMALLINT    NOT NULL,
    ref_month       SMALLINT,
    program_stream  VARCHAR     NOT NULL,
    country         VARCHAR     NOT NULL,
    province        VARCHAR     NOT NULL DEFAULT '_',
    count           INTEGER,
    source_dataset  VARCHAR     NOT NULL DEFAULT 'IRCC-360024f2',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_year, ref_month, program_stream, country, province)
);

CREATE TABLE IF NOT EXISTS work_permit_programs_monthly (
    ref_year        SMALLINT    NOT NULL,
    ref_month       SMALLINT,
    program_stream  VARCHAR     NOT NULL,
    count           INTEGER,
    source_dataset  VARCHAR     NOT NULL DEFAULT 'IRCC-360024f2',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_year, ref_month, program_stream)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- ASYLUM CLAIMANTS (Monthly IRCC)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS asylum_claimants_monthly (
    ref_year        SMALLINT    NOT NULL,
    ref_month       SMALLINT,
    country         VARCHAR     NOT NULL DEFAULT '_',
    province        VARCHAR     NOT NULL DEFAULT '_',
    count           INTEGER,
    source_dataset  VARCHAR     NOT NULL DEFAULT 'IRCC-b6cbcf4d',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_year, ref_month, country, province)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- PERMANENT RESIDENTS (Monthly IRCC)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS permanent_residents_monthly (
    ref_year        SMALLINT    NOT NULL,
    ref_month       SMALLINT,
    immigration_category VARCHAR NOT NULL,
    country         VARCHAR     NOT NULL,
    province        VARCHAR     NOT NULL DEFAULT '_',
    count           INTEGER,
    source_dataset  VARCHAR     NOT NULL DEFAULT 'IRCC-f7e5498e',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (ref_year, ref_month, immigration_category, country, province)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- COUNTRY OF ORIGIN — NATURALIZED (Census)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS naturalized_by_country_census (
    census_year     SMALLINT    NOT NULL,
    country         VARCHAR     NOT NULL,
    count           BIGINT,
    source_table    VARCHAR     NOT NULL DEFAULT '98-10-0304-01',
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (census_year, country)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- PIPELINE RUN LOG
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR     NOT NULL DEFAULT uuid(),
    started_at      TIMESTAMP   NOT NULL DEFAULT now(),
    finished_at     TIMESTAMP,
    status          VARCHAR     NOT NULL DEFAULT 'running',
    sources_fetched VARCHAR[],
    notes           VARCHAR,
    PRIMARY KEY (run_id)
);

CREATE TABLE IF NOT EXISTS source_loads (
    load_id         VARCHAR     NOT NULL DEFAULT uuid(),
    run_id          VARCHAR     NOT NULL,
    dataset_key     VARCHAR     NOT NULL,
    source_family   VARCHAR     NOT NULL,
    source_id       VARCHAR     NOT NULL,
    resource_id     VARCHAR,
    reference_period VARCHAR,
    cadence         VARCHAR,
    status          VARCHAR     NOT NULL,
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    notes           VARCHAR,
    PRIMARY KEY (load_id)
);
