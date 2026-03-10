curl -O https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/8.2.0/confluentinc-kafka-connect-avro-converter-8.2.0.zip
unzip confluentinc-kafka-connect-avro-converter-8.2.0.zip
mv confluentinc-kafka-connect-avro-converter-8.2.0 avro-converter
rm confluentinc-kafka-connect-avro-converter-8.2.0.zip


curl -O https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/12.1.1/confluentinc-kafka-connect-s3-12.1.1.zip
unzip confluentinc-kafka-connect-s3-12.1.1.zip
mv confluentinc-kafka-connect-s3-12.1.1 s3-connector
rm confluentinc-kafka-connect-s3-12.1.1.zip