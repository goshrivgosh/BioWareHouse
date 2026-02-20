DROP TABLE IF EXISTS stage_raw_minio_objects CASCADE;
CREATE TABLE stage_raw_minio_objects (
	stage_event_uuid uuid default gen_random_uuid() PRIMARY KEY,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_data JSONB
);

DROP TABLE IF EXISTS ods_raw_minio_objects CASCADE;
CREATE TABLE ods_raw_minio_objects (
	ods_event_uuid uuid default gen_random_uuid(),
    key VARCHAR(2048) PRIMARY KEY,  
    value JSONB NOT NULL            
);

DROP VIEW IF EXISTS stage_parsed_minio_objects CASCADE;
CREATE VIEW stage_parsed_minio_objects AS
SELECT 
    stage_event_uuid,
    (event_data->'Records'->0->>'eventTime')::TIMESTAMP as event_time,
    event_data->'Records'->0->>'eventName' as event_type,
    event_data->'Records'->0->'s3'->'bucket'->>'name' as bucket_name,
    event_data->'Records'->0->'s3'->'object'->>'key' as object_key,
    (event_data->'Records'->0->'s3'->'object'->>'size')::BIGINT as object_size,
    event_data->'Records'->0->'s3'->'object'->>'contentType' as content_type,
    event_data->'Records'->0->'source'->>'host' as source_ip
FROM stage_raw_minio_objects;

DROP view IF EXISTS ods_parsed_minio_objects CASCADE;
CREATE VIEW ods_parsed_minio_objects AS
SELECT 
    ods_event_uuid,
    key,
    (value->'Records'->0->>'eventTime')::TIMESTAMP as event_time,
    value->'Records'->0->>'eventName' as event_type,
    value->'Records'->0->'s3'->'bucket'->>'name' as bucket_name,
    value->'Records'->0->'s3'->'object'->>'key' as object_key,
    (value->'Records'->0->'s3'->'object'->>'size')::BIGINT as object_size,
    value->'Records'->0->'s3'->'object'->>'contentType' as content_type,
    value->'Records'->0->'source'->>'host' as source_ip
FROM ods_raw_minio_objects;
