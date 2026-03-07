package com.stock;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StockProcessingJob {

    // ── Kafka + Schema Registry ───────────────────────────────────────
    private static final String BOOTSTRAP_SERVERS =
            "kafka-1:29092,kafka-2:29092,kafka-3:29092";
    private static final String SCHEMA_REGISTRY_URL =
            "http://schema-registry:8081";

    // ─────────────────────────────────────────────────────────────────
    // KafkaDeserializationSchema: dùng Confluent KafkaAvroDeserializer
    // ─────────────────────────────────────────────────────────────────
    public static class ConfluentGenericRecordDeserSchema implements KafkaDeserializationSchema<GenericRecord> {

        private final String schemaRegistryUrl;
        private transient KafkaAvroDeserializer deserializer;

        public ConfluentGenericRecordDeserSchema(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        private void initIfNeeded() {
            if (deserializer != null) return;

            Map<String, Object> cfg = new HashMap<>();
            cfg.put("schema.registry.url", schemaRegistryUrl);
            cfg.put("specific.avro.reader", false); // trả về GenericRecord

            deserializer = new KafkaAvroDeserializer();
            deserializer.configure(cfg, false); // false = value deserializer
        }

        @Override
        public GenericRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            initIfNeeded();
            Object obj = deserializer.deserialize(record.topic(), record.value());
            return (obj instanceof GenericRecord) ? (GenericRecord) obj : null;
        }

        @Override
        public boolean isEndOfStream(GenericRecord nextElement) {
            return false;
        }

        @Override
        public TypeInformation<GenericRecord> getProducedType() {
            return Types.GENERIC(GenericRecord.class);
        }
    }

    // ── ScyllaDB Sink ─────────────────────────────────────────────────
    public static class ScyllaSinkFunction extends RichSinkFunction<GenericRecord> {

        private static final String[] CONTACT_POINTS = {"scylla-node1", "scylla-node2", "scylla-node3"};
        private static final int PORT = 9042;
        private static final String KEYSPACE = "stock_data";

        private transient Cluster cluster;
        private transient Session session;
        private transient PreparedStatement insertPriceStmt;
        private transient PreparedStatement upsertLatestStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Exception lastEx = null;
            for (int i = 0; i < 30; i++) {
                try {
                    cluster = Cluster.builder()
                            .addContactPoints(CONTACT_POINTS)
                            .withPort(PORT)
                            .build();
                    session = cluster.connect(KEYSPACE);
                    System.out.println("✅ Connected to ScyllaDB");
                    break;
                } catch (Exception e) {
                    lastEx = e;
                    System.out.println("⏳ ScyllaDB not ready, retry " + (i + 1) + "/30 ...");
                    if (cluster != null) {
                        try { cluster.close(); } catch (Exception ignored) {}
                        cluster = null;
                    }
                    Thread.sleep(5000);
                }
            }
            if (session == null) {
                throw new RuntimeException("Cannot connect to ScyllaDB after 30 retries", lastEx);
            }

            insertPriceStmt = session.prepare(
                    "INSERT INTO stock_prices "
                            + "(symbol, timestamp, price, exchange, quote_type, market_hours, "
                            + "change_percent, change, price_hint, producer_timestamp, day_volume, last_size) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );

            upsertLatestStmt = session.prepare(
                    "INSERT INTO stock_latest_prices "
                            + "(symbol, price, timestamp, exchange, quote_type, market_hours, "
                            + "change_percent, change, price_hint, producer_timestamp, day_volume, last_size) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );
        }

        @Override
        public void invoke(GenericRecord record, Context context) {
            if (record == null) return;
            try {
                String symbol     = str(record, "id");
                Double price      = dbl(record, "price");
                Long   timeMs     = lng(record, "time");
                String exchange   = str(record, "exchange");
                Integer quoteType = integer(record, "quote_type");
                Integer mktHours  = integer(record, "market_hours");
                Double changePct  = dbl(record, "change_percent");
                Double change     = dbl(record, "change");
                String priceHint  = str(record, "price_hint");
                Long   receivedAt = lng(record, "received_at");
                Long   dayVolume  = safeLng(record, "day_volume");
                Long   lastSize   = safeLng(record, "last_size");

                // stock_prices: timestamp TEXT (epoch ms dạng chuỗi)
                String tsText = (timeMs != null)
                        ? String.valueOf(timeMs)
                        : String.valueOf(System.currentTimeMillis());

                session.executeAsync(insertPriceStmt.bind(
                        symbol, tsText, price, exchange,
                        quoteType, mktHours, changePct, change,
                        priceHint, receivedAt, dayVolume, lastSize
                ));

                // stock_latest_prices: timestamp TIMESTAMP (java.util.Date)
                Date tsDate = (timeMs != null) ? new Date(timeMs) : new Date();

                session.executeAsync(upsertLatestStmt.bind(
                        symbol, price, tsDate, exchange,
                        quoteType, mktHours, changePct, change,
                        priceHint, receivedAt, dayVolume, lastSize
                ));

            } catch (Exception e) {
                System.err.println("❌ ScyllaDB write error: " + e.getMessage());
            }
        }

        @Override
        public void close() {
            if (session != null) { try { session.close(); } catch (Exception ignored) {} }
            if (cluster != null) { try { cluster.close(); } catch (Exception ignored) {} }
        }

        private static String  str(GenericRecord r, String f)     { Object v = r.get(f); return v != null ? v.toString() : null; }
        private static Double  dbl(GenericRecord r, String f)     { Object v = r.get(f); return v instanceof Number ? ((Number) v).doubleValue() : null; }
        private static Long    lng(GenericRecord r, String f)     { Object v = r.get(f); return v instanceof Number ? ((Number) v).longValue()   : null; }
        private static Integer integer(GenericRecord r, String f) { Object v = r.get(f); return v instanceof Number ? ((Number) v).intValue()    : null; }
        /** Safely get a Long field — returns null if the field doesn't exist in schema */
        private static Long safeLng(GenericRecord r, String f) {
            try { if (r.getSchema().getField(f) == null) return null; return lng(r, f); }
            catch (Exception e) { return null; }
        }
    }

    // ── ClickHouse Sink ───────────────────────────────────────────────
    public static class ClickHouseSinkFunction extends RichSinkFunction<GenericRecord> {

        private static final String CH_URL = "jdbc:clickhouse://clickhouse:8123/stock_warehouse";
        private static final String CH_USER = "default";
        private static final String CH_PASS = "truongittstock";

        private transient Connection connection;
        private transient java.sql.PreparedStatement insertStmt;

        private static final int BATCH_SIZE = 500;
        private transient int batchCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Exception lastEx = null;
            for (int i = 0; i < 30; i++) {
                try {
                    connection = DriverManager.getConnection(CH_URL, CH_USER, CH_PASS);
                    System.out.println("✅ Connected to ClickHouse");
                    break;
                } catch (Exception e) {
                    lastEx = e;
                    System.out.println("⏳ ClickHouse not ready, retry " + (i + 1) + "/30 ...");
                    Thread.sleep(5000);
                }
            }
            if (connection == null) {
                throw new RuntimeException("Cannot connect to ClickHouse after 30 retries", lastEx);
            }

            insertStmt = connection.prepareStatement(
                    "INSERT INTO stock_ticks "
                            + "(symbol, price, event_time, exchange, quote_type, market_hours, "
                            + "change_percent, change, price_hint, received_at, day_volume, last_size) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );
            batchCount = 0;
        }

        @Override
        public void invoke(GenericRecord record, Context context) {
            if (record == null) return;
            try {
                String symbol     = str(record, "id");
                Double price      = dbl(record, "price");
                Long   timeMs     = lng(record, "time");
                String exchange   = str(record, "exchange");
                Integer quoteType = integer(record, "quote_type");
                Integer mktHours  = integer(record, "market_hours");
                Double changePct  = dbl(record, "change_percent");
                Double change     = dbl(record, "change");
                String priceHint  = str(record, "price_hint");
                Long   receivedAt = lng(record, "received_at");
                Long   dayVolume  = safeLng(record, "day_volume");
                Long   lastSize   = safeLng(record, "last_size");

                long eventMs = (timeMs != null) ? timeMs : System.currentTimeMillis();
                long recvMs  = (receivedAt != null) ? receivedAt : System.currentTimeMillis();

                insertStmt.setString(1, symbol);
                insertStmt.setDouble(2, price != null ? price : 0.0);
                insertStmt.setTimestamp(3, new Timestamp(eventMs));
                insertStmt.setString(4, exchange);
                insertStmt.setObject(5, quoteType);
                insertStmt.setObject(6, mktHours);
                insertStmt.setObject(7, changePct);
                insertStmt.setObject(8, change);
                insertStmt.setString(9, priceHint);
                insertStmt.setTimestamp(10, new Timestamp(recvMs));
                insertStmt.setObject(11, dayVolume);
                insertStmt.setObject(12, lastSize);

                insertStmt.addBatch();
                batchCount++;

                if (batchCount >= BATCH_SIZE) {
                    insertStmt.executeBatch();
                    batchCount = 0;
                }

            } catch (Exception e) {
                System.err.println("❌ ClickHouse write error: " + e.getMessage());
            }
        }

        @Override
        public void close() {
            try {
                if (insertStmt != null && batchCount > 0) {
                    insertStmt.executeBatch();
                }
            } catch (Exception ignored) {}
            try { if (insertStmt != null) insertStmt.close(); } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }

        private static String  str(GenericRecord r, String f)     { Object v = r.get(f); return v != null ? v.toString() : null; }
        private static Double  dbl(GenericRecord r, String f)     { Object v = r.get(f); return v instanceof Number ? ((Number) v).doubleValue() : null; }
        private static Long    lng(GenericRecord r, String f)     { Object v = r.get(f); return v instanceof Number ? ((Number) v).longValue()   : null; }
        private static Integer integer(GenericRecord r, String f) { Object v = r.get(f); return v instanceof Number ? ((Number) v).intValue()    : null; }
        /** Safely get a Long field — returns null if the field doesn't exist in schema */
        private static Long safeLng(GenericRecord r, String f) {
            try { if (r.getSchema().getField(f) == null) return null; return lng(r, f); }
            catch (Exception e) { return null; }
        }
    }

    // ── MAIN ──────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        // Register Avro Kryo serializers (fix KryoException with GenericRecord)
        env.getConfig().addDefaultKryoSerializer(
                org.apache.avro.Schema.class,
                org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils.AvroSchemaSerializer.class);
        env.getConfig().addDefaultKryoSerializer(
                org.apache.avro.generic.GenericData.Array.class,
                org.apache.flink.api.java.typeutils.runtime.kryo.Serializers.SpecificInstanceCollectionSerializerForArrayList.class);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaProps.setProperty("group.id", "flink-stock-processor");

        // Tạo 2 deserializer riêng (an toàn hơn)
        FlinkKafkaConsumer<GenericRecord> vnConsumer = new FlinkKafkaConsumer<>(
                "stock_price_vn",
                new ConfluentGenericRecordDeserSchema(SCHEMA_REGISTRY_URL),
                kafkaProps
        );
        vnConsumer.setStartFromEarliest();

        FlinkKafkaConsumer<GenericRecord> difConsumer = new FlinkKafkaConsumer<>(
                "stock_price_dif",
                new ConfluentGenericRecordDeserSchema(SCHEMA_REGISTRY_URL),
                kafkaProps
        );
        difConsumer.setStartFromEarliest();

        DataStream<GenericRecord> vnStream  = env.addSource(vnConsumer).name("Kafka VN Stocks");
        DataStream<GenericRecord> difStream = env.addSource(difConsumer).name("Kafka International Stocks");

        DataStream<GenericRecord> allStocks = vnStream.union(difStream);

        allStocks
                .map(r -> r == null ? "[STOCK] null" :
                        String.format("[STOCK] %s = %s (%s)", r.get("id"), r.get("price"), r.get("exchange")))
                .returns(Types.STRING)
                .print();

        allStocks.addSink(new ScyllaSinkFunction())
                .name("ScyllaDB Sink")
                .setParallelism(2);

        allStocks.addSink(new ClickHouseSinkFunction())
                .name("ClickHouse Sink")
                .setParallelism(2);

        env.execute("Stock Price Processing");
    }
}