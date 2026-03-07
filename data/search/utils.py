from confluent_kafka import Producer, Consumer, TopicPartition

def get_partition_with_least_messages(topic_name, consumer, admin_client, logger=None):
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            logger.error(f"🔴 Topic '{topic_name}' does not exist.")
            return 0

        topic_meta = metadata.topics[topic_name]
        partition_offsets = {}

        topic_partitions = [TopicPartition(topic_name, p) for p in topic_meta.partitions]
        consumer.assign(topic_partitions) 
        consumer.poll(1.0) 
        logger.info(f"📊 Checking message counts for topic '{topic_name}' across {len(topic_meta.partitions)} partitions:")
        for p in topic_meta.partitions:
            tp = TopicPartition(topic_name, p)
            try:
                low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
                message_count = high - low 
                partition_offsets[p] = message_count
                logger.info(f"📦 Partition {p}: First Offset = {low}, Next Offset = {high}, Message Count = {message_count}")
            except Exception as e:
                logger.warning(f"⚠️ Could not get offsets for partition {p}: {e}")
                partition_offsets[p] = float('inf')  
        if not partition_offsets:
            logger.error("🔴 No valid partitions found.")
            return 0
        min_partition = min(partition_offsets, key=partition_offsets.get)
        logger.info(f"✅ Partition with least messages: {min_partition} (Message Count = {partition_offsets[min_partition]})")
        return min_partition
    except Exception as e:
        logger.error(f"🔴 Error determining least busy partition: {e}")
        return 0
