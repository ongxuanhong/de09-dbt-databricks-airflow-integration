import os
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from delta_sink import DeltaSink

connection_config = ConnectionConfig(
    bootstrap_servers="localhost:29092",
)

# Sink to Delta table
bucket_name = "silver"

app_kwargs = {
    "broker_address": connection_config,
    "consumer_group": "stream-ingestions",
    "auto_offset_reset": "earliest",
}
app = Application(**app_kwargs)

topic_name = "postgres1.inventory.customers"
delta_table_uri = "s3://{bucket_name}/{topic_name}".format(bucket_name=bucket_name, topic_name=topic_name)
print(f"Bucket: {bucket_name}, Delta table URI: {delta_table_uri}")
print(f"Topic: {topic_name}")

delta_sink = DeltaSink(
    table_uri=delta_table_uri,
    timestamp_column="ts_ms",
    partition_columns=("year", "month", "day"),
    payload_key="after",  # Debezium CDC: store "after" row as columns (id, first_name, ...)
)
topic = app.topic(topic_name)
sdf = app.dataframe(topic)
sdf.sink(delta_sink)

if __name__ == "__main__":
    app.run()
