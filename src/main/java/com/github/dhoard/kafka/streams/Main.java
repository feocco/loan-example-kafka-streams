package com.github.dhoard.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dhoard.kafka.streams.serde.JsonNodeArrayListDeserializer;
import com.github.dhoard.kafka.streams.serde.JsonNodeArrayListSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.happypants.demo.des.serde.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final Serde<JsonNode> jsonSerde = new JsonSerde();

    private static final Serializer<ArrayList<JsonNode>> arrayListSerializer = new JsonNodeArrayListSerializer<>();

    private static final Deserializer<ArrayList<JsonNode>> arrayListDeserializer = new JsonNodeArrayListDeserializer<>();

    private static final Serde<ArrayList<JsonNode>> arrayListSerde = Serdes.serdeFrom(arrayListSerializer, arrayListDeserializer);

    public static void main(String[] args) throws Throwable {
        new Main().run(args);
    }

    public void run(String[] args) throws Throwable {
        LOGGER.info("Application starting");

        String bootstrapServers = "BROKER:9092";
        String applicationId = "loan-example-kafka-streams";
        Class keySerde = Serdes.String().getClass();
        Class valueSerde = JsonSerde.class;
        String autoOffsetResetConfig = "earliest";
        String topicName = "ingest";
        int numStreamThreads = Runtime.getRuntime().availableProcessors() * 2;
        long cacheMaxBytesBuffering = 0;
        String stateDir = "/tmp";

        LOGGER.info("bootstrapServers       = [" + bootstrapServers + "]");
        LOGGER.info("autoOffsetResetConfig  = [" + autoOffsetResetConfig + "]");
        LOGGER.info("applicationId          = [" + applicationId + "]");
        LOGGER.info("defaultKeySerde        = [" + keySerde + "]");
        LOGGER.info("defaultValueSerde      = [" + valueSerde + "]");
        LOGGER.info("topicName              = [" + topicName + "]");
        LOGGER.info("numStreamThreads       = [" + numStreamThreads + "]");
        LOGGER.info("cacheMaxBytesBuffering = [" + cacheMaxBytesBuffering + "]");
        LOGGER.info("stateDir               = [" + stateDir + "]");

        Properties properties = new Properties();
        
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);

        properties.put(
                "default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        properties.put(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                WallclockTimestampExtractor.class);

        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytesBuffering);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        Topology topology = getTopology(topicName);
        LOGGER.warn(topology.describe().toString());
        
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setGlobalStateRestoreListener(
            new StateRestoreListener() {
                @Override
                public void onRestoreStart(TopicPartition topicPartition, String s, long l,
                    long l1) {
                    LOGGER.info(
                        "onRestoreStart() topicPartition = [" + topicPartition.partition() + "]"
                        + " storeName = [" + s + "]"
                        + " offsets = [" + l + " -> " + l1 + "]");
                }

                @Override
                public void onBatchRestored(TopicPartition topicPartition, String s, long l,
                    long l1) {
                    // DO NOTHING
                }

                @Override
                public void onRestoreEnd(TopicPartition topicPartition, String s, long l) {
                    LOGGER.info("onRestoreEnd() topicPartition = [" + topicPartition.partition()
                        + " storeName = [" + s + "]"
                        + " endOffset = [" + l + "]");
                }
            }
        );

        kafkaStreams.start();

        while (true) {
            KafkaStreams.State state = kafkaStreams.state();

            if ("RUNNING".equals(state.toString())) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // DO NOTHING
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        while (true) {
            try {
                Thread.sleep(100);
            } catch (Throwable t) {
                // DO NOTHING
            }
        }
    }

    private Topology getTopology(String topic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Create a KStreams object from the ingestion topic
        KStream<String, JsonNode> ingestionKStream =
            streamsBuilder.stream(topic, Consumed.with(Serdes.String(), jsonSerde));


        // Branch (i.e. split) the ingestion KStream to 2 KStreams
        //
        // branchedKStreams[0] contains "LOAN_GROUP" events
        // branchedKStreams[1] contains "LOAN" events
        KStream<String, JsonNode>[] branchedKStreams =
            ingestionKStream.branch(Named.as(topic),
                (key, jsonNode) -> "LOAN_GROUP".equals(jsonNode.path("type").asText()),
                (key, jsonNode) -> "LOAN".equals(jsonNode.path("type").asText()));

        KStream<String, JsonNode> loanGroupKStream = branchedKStreams[0];
        KStream<String, JsonNode> loansKStream = branchedKStreams[1];

        // Sink the branched KStreams to topics
        loansKStream.to("loan");
        loanGroupKStream.to("loan-group");

        // Create a KTable for the loan processed events stream
        // External client is processing loan topic and outputting to loan-processed
        KTable<String, JsonNode> loansKTable = streamsBuilder.table("loan-processed");

        // Create KStream for pool processed events stream
        // External client is processing loan-group topic and outputting to loan-group-processed
        KStream<String, JsonNode> loanGroupProcessedKStream =
                streamsBuilder.stream("loan-group-processed");

        // Create a KTable from the rekeyed loan group stream
        KTable<String, JsonNode> loanIdKeyedLoanGroupKTable = loanGroupProcessedKStream.flatMap(
                (key, value) -> {
                    List<KeyValue<String, JsonNode>>  keyValueList = new ArrayList<>();
                    String loanGroupId = value.get("id").asText();
                    JsonNode jsonNode = value.get("loanIds");
                    int loanIdCount = jsonNode.size();
                    for (int i = 0; i < loanIdCount; i++) {
                        String loanId = jsonNode.get(i).asText();
                        KeyValue<String, JsonNode> keyValue = new KeyValue<>(loanId, value);
                        keyValueList.add(keyValue);
                    }

                    return keyValueList;
                }).toTable(Materialized.with(Serdes.String(), jsonSerde));

        KTable<String, JsonNode> enrichedLoanGroupKTable =
            loanIdKeyedLoanGroupKTable.join(
                loansKTable,
                (loanGroupJsonNode, loanJsonNode) -> {
                    ObjectNode loanGroupObjectNode = (ObjectNode) loanGroupJsonNode;
                    JsonNode result = loanGroupObjectNode.set(
                        "loan-" + loanJsonNode.get("id").asText(),
                        loanJsonNode);

                    return result;
                });

        KTable<String, ArrayList<JsonNode>> enrichedCompleteLoanGroupKTable =
            enrichedLoanGroupKTable
                .toStream()
                .groupBy((k, v) -> v.path("id").asText())
                .aggregate(ArrayList::new,
                    (s, jsonNode, jsonNodes) -> {
                        jsonNodes.add(jsonNode);
                        return jsonNodes;
                    }
                    , Materialized.with(Serdes.String(), arrayListSerde))
                .filterNot((k, v) -> v == null)
                .filter((k, v) -> v.size() == v.get(0).get("loanCount").asInt());

        enrichedCompleteLoanGroupKTable.toStream().to("loan-group-complete");

        return streamsBuilder.build();
    }
}
