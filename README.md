# de09-dbt-databricks-airflow-integration

## Use case summary
- CDC push messages to Kafka topics
- Kafka connect store to bronze layer in Avro format
- Prepare Databricks Unity Catalog (bronze, silver, gold)
- DBT load and parse CDC schemas and store to silver layer
- DBT run SCD type 1 to silver layer
- Airflow integrate with DBT and Databricks
- CI/CD when changes

## Services preparation
```bash
chmod +x ./download_connects.sh
./download_connects.sh

docker compose up -d

# Check services
docker compose ps

# check postgres
docker compose exec postgres psql -U postgres -c "SELECT * FROM inventory.customers;"

# check kafka topics
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# See all groups
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list

# Describe one group (optional – see if active members / offsets exist)
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group console-consumer-15706

# Delete
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --delete --group console-consumer-15706

# Create connector for PostgreSQL debezium_demo database
# JSON
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "debezium-demo-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "postgres",

      "topic.prefix": "postgres1",

      "plugin.name": "pgoutput",
      "slot.name": "debezium_demo_slot",
      "publication.name": "debezium_demo_pub",
      "publication.autocreate.mode": "filtered",

      "schema.include.list": "inventory",
      "table.include.list": "inventory.customers,inventory.orders",
      "snapshot.mode": "initial",

      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }'

# Avro
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "debezium-demo-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "postgres",

      "topic.prefix": "postgres1",

      "plugin.name": "pgoutput",
      "slot.name": "debezium_demo_slot",
      "publication.name": "debezium_demo_pub",
      "publication.autocreate.mode": "filtered",

      "schema.include.list": "inventory",
      "table.include.list": "inventory.customers,inventory.orders",
      "snapshot.mode": "initial",

      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "http://schema-registry:8082",
      "value.converter.schema.registry.url": "http://schema-registry:8082"
    }
  }'  

# Check connectors
curl -XGET http://localhost:8083/connectors

# Retrieves additional state information for each connector and its tasks
curl -XGET http://localhost:8083/connectors?expand=status | jq

# Returns metadata for each connector (config, tasks, type)
curl -XGET http://localhost:8083/connectors?expand=info | jq

# delete
curl -i -X DELETE http://localhost:8083/connectors/debezium-demo-connector


# inspecting
curl -s http://localhost:8083/connectors/debezium-demo-connector/status | jq
curl -s http://localhost:8083/connectors/debezium-demo-connector | jq '.config'  

# Observing CDC with CRUD
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic postgres1.inventory.customers --from-beginning | jq

# Create
docker exec postgres psql -U postgres -d postgres -c "
INSERT INTO inventory.customers(first_name,last_name,email)
VALUES ('CDC','Create','cdc.create@example.com');
"

# Update
docker exec postgres psql -U postgres -d postgres -c "
UPDATE inventory.customers
SET first_name='CDC-Updated', email='cdc.updated@example.com'
WHERE email='cdc.create@example.com';
"

# Delete
docker exec postgres psql -U postgres -d postgres -c "
DELETE FROM inventory.customers
WHERE email='cdc.updated@example.com';
"
```

## Data preparation
```bash
# S3 Connector
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-west-2
export S3_BUCKET_NAME=bronze

# Avro topic to Avro file in S3
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "s3-sink-connector",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics.regex": "postgres1\\..*",

      "s3.region": "${AWS_REGION}",
      "s3.bucket.name": "${S3_BUCKET_NAME}",
      "aws.access.key.id": "${AWS_ACCESS_KEY_ID}",
      "aws.secret.access.key": "${AWS_SECRET_ACCESS_KEY}",

      "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DailyPartitioner",
      "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
      "partition.duration.ms": "86400000",
      "locale": "en-US",
      "timezone": "UTC",

      "schema.compatibility": "NONE",
      "topics.dir": "event_streaming",

      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "http://schema-registry:8082",
      "value.converter.schema.registry.url": "http://schema-registry:8082",

      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "flush.size": "4",
      "rotate.schedule.interval.ms": "3600000",
      "consumer.override.auto.offset.reset": "earliest"
    }
  }'

# JSON topic to Avro file in S3  
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "s3-sink-connector",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics.regex": "postgres1\\..*",

      "store.url": "http://minio:9000",
      "s3.region": "us-west-2",
      "s3.bucket.name": "bronze",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",

      "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DailyPartitioner",
      "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
      "partition.duration.ms": "86400000",
      "locale": "en-US",
      "timezone": "UTC",
      "behavior.on.null.values": "ignore",

      "schema.compatibility": "NONE",
      "topics.dir": "event_streaming",

      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",

      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "flush.size": "4",
      "rotate.schedule.interval.ms": "3600000",
      "consumer.override.auto.offset.reset": "earliest"
    }
  }'

# JSON topic to JSON file in S3  
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "s3-sink-connector",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics.regex": "postgres1\\..*",

      "store.url": "http://minio:9000",
      "s3.region": "us-west-2",
      "s3.bucket.name": "bronze",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",

      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DailyPartitioner",
      "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
      "partition.duration.ms": "86400000",
      "locale": "en-US",
      "timezone": "UTC",
      "behavior.on.null.values": "ignore",

      "schema.compatibility": "NONE",
      "topics.dir": "event_streaming",

      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",

      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "flush.size": "1",
      "rotate.schedule.interval.ms": "3600000",
      "consumer.override.auto.offset.reset": "earliest"
    }
  }'


curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "s3-sink-connector",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics.regex": "postgres1\\..*",

      "store.url": "http://minio:9000",
      "s3.region": "us-west-2",
      "s3.bucket.name": "bronze",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",
      "s3.part.size" : "5242880",

      "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
      "avro.codec": "snappy",
      "storage.class" : "io.confluent.connect.s3.storage.S3Storage",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DailyPartitioner",
      "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
      "partition.duration.ms" : "3600000",
      "timestamp.extractor" : "Record",
      "locale" : "en-US",
      "timezone" : "UTC",
      "flush.size" : "1",
      "rotate.interval.ms" : "300000",
      "rotate.schedule.interval.ms" : "3600000",
      "topics.dir" : "event_streaming",
      "schema.generator.class" : "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
      "schema.compatibility" : "BACKWARD",
      "max.retries" : "10",
      "retry.backoff.ms" : "5000",
      "errors.tolerance" : "none",
      "errors.log.enable" : "true",
      "errors.log.include.messages" : "true",
      "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable" : "false",
      "value.converter" : "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable" : "false"
    }
  }'  

# delete
curl -X DELETE http://localhost:8083/connectors/s3-sink-connector  
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list | grep connect
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --delete --group connect-s3-sink-connector

# check
aws s3 ls $S3_BUCKET_NAME/topics/postgres1.inventory.customers/year=2026/month=03/day=10/
aws s3 ls $S3_BUCKET_NAME/event_streaming/postgres1.inventory.customers/year=2026/month=03/day=10/
```

## DBT preparation
```bash
uv venv --python 3.12 && source .venv/bin/activate
uv pip install -r requirements.txt

dbt init dbx_warehouse
cd dbx_warehouse/

export DBT_CATALOG=
export DBT_HOST=
export DBT_HTTP_PATH=
export DBT_SCHEMA=
export DBT_TOKEN=

# materialize tables and views
dbt run --profiles-dir ./

```

## Airflow integration
```bash
cd airflow_project
docker compose build
docker compose up -d
```

## CI/CD integration