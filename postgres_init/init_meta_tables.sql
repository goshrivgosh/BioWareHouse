DROP TABLE IF EXISTS stage_vcf_object CASCADE;
CREATE TABLE stage_vcf_object (
	stage_vcf_event_uuid uuid default gen_random_uuid() PRIMARY KEY,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_data JSONB
);

DROP TABLE IF EXISTS ods_vcf_object CASCADE;
CREATE TABLE ods_vcf_object (
	ods_vcf_event_uuid uuid default gen_random_uuid(),
    key VARCHAR(2048) PRIMARY KEY,
    value JSONB NOT NULL
);

DROP TABLE IF EXISTS stage_parquet_object CASCADE;
CREATE TABLE stage_parquet_object (
	stage_parquet_event_uuid uuid default gen_random_uuid() PRIMARY KEY,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_data JSONB
);

DROP TABLE IF EXISTS ods_parquet_object CASCADE;
CREATE TABLE ods_parquet_object (
	ods_parquet_event_uuid uuid default gen_random_uuid(),
    key VARCHAR(2048) PRIMARY KEY,
    value JSONB NOT NULL
);

DROP VIEW IF EXISTS stage_vcf_parsed_object CASCADE;
CREATE VIEW stage_vcf_parsed_object AS
SELECT
    stage_vcf_event_uuid,
    (event_data->'Records'->0->>'eventTime')::TIMESTAMP as event_time,
    event_data->'Records'->0->>'eventName' as event_type,
    event_data->'Records'->0->'s3'->'bucket'->>'name' as bucket_name,
    event_data->'Records'->0->'s3'->'object'->>'key' as object_key,
    (event_data->'Records'->0->'s3'->'object'->>'size')::BIGINT as object_size,
    event_data->'Records'->0->'s3'->'object'->>'contentType' as content_type,
    event_data->'Records'->0->'source'->>'host' as source_ip
FROM stage_vcf_object;

DROP view IF EXISTS ods_vcf_parsed_object CASCADE;
CREATE VIEW ods_vcf_parsed_object AS
SELECT
    ods_vcf_event_uuid,
    key,
    (value->'Records'->0->>'eventTime')::TIMESTAMP as event_time,
    value->'Records'->0->>'eventName' as event_type,
    value->'Records'->0->'s3'->'bucket'->>'name' as bucket_name,
    value->'Records'->0->'s3'->'object'->>'key' as object_key,
    (value->'Records'->0->'s3'->'object'->>'size')::BIGINT as object_size,
    value->'Records'->0->'s3'->'object'->>'contentType' as content_type,
    value->'Records'->0->'source'->>'host' as source_ip
FROM ods_vcf_object;

DROP VIEW IF EXISTS stage_parquet_parsed_object CASCADE;
CREATE VIEW stage_parquet_parsed_object AS
SELECT
    stage_parquet_event_uuid,
    (event_data->'Records'->0->>'eventTime')::TIMESTAMP as event_time,
    event_data->'Records'->0->>'eventName' as event_type,
    event_data->'Records'->0->'s3'->'bucket'->>'name' as bucket_name,
    event_data->'Records'->0->'s3'->'object'->>'key' as object_key,
    (event_data->'Records'->0->'s3'->'object'->>'size')::BIGINT as object_size,
    event_data->'Records'->0->'s3'->'object'->>'contentType' as content_type,
    event_data->'Records'->0->'source'->>'host' as source_ip
FROM stage_parquet_object;

DROP view IF EXISTS ods_parquet_parsed_object CASCADE;
CREATE VIEW ods_parquet_parsed_object AS
SELECT
    ods_parquet_event_uuid,
    key,
    (value->'Records'->0->>'eventTime')::TIMESTAMP as event_time,
    value->'Records'->0->>'eventName' as event_type,
    value->'Records'->0->'s3'->'bucket'->>'name' as bucket_name,
    value->'Records'->0->'s3'->'object'->>'key' as object_key,
    (value->'Records'->0->'s3'->'object'->>'size')::BIGINT as object_size,
    value->'Records'->0->'s3'->'object'->>'contentType' as content_type,
    value->'Records'->0->'source'->>'host' as source_ip
FROM ods_parquet_object;
