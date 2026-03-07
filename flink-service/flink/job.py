from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import AvroDeserializationSchema
from pyflink.common.typeinfo import Types

def create_kafka_source(topic, schema_registry_url):
    return KafkaSource.builder()\
        .set_bootstrap_servers("kafka-1:29092")\
        .set_topics(topic)\
        .set_group_id("flink_consumer_group")\
        .set_value_only_deserializer(
            AvroDeserializationSchema.for_generic(
                schema_registry_url=schema_registry_url,
                subject=f"{topic}-value"
            )
        )\
        .build()

def main():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Define Kafka sources for both topics
    schema_registry_url = "http://schema-registry:8081"
    kafka_source_vn = create_kafka_source("stock_price_vn", schema_registry_url)
    kafka_source_dif = create_kafka_source("stock_price_dif", schema_registry_url)

    # Add Kafka sources to the environment
    ds_vn = env.from_source(
        kafka_source_vn,
        watermark_strategy=None,  # Add watermark strategy if needed
        type_info=Types.STRING()
    )

    ds_dif = env.from_source(
        kafka_source_dif,
        watermark_strategy=None,  # Add watermark strategy if needed
        type_info=Types.STRING()
    )

    # Print the data to console
    ds_vn.print("Stock Price VN")
    ds_dif.print("Stock Price DIF")

    # Execute the Flink job
    env.execute("Kafka Avro Flink Job for Multiple Topics")

if __name__ == "__main__":
    main()