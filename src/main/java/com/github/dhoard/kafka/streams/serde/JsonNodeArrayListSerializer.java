package com.github.dhoard.kafka.streams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class JsonNodeArrayListSerializer<T> implements Serializer<ArrayList<JsonNode>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // DO NOTHNG
    }

    @Override
    public byte[] serialize(String topic, ArrayList<JsonNode> arrayList) {
        ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
        arrayNode.addAll(arrayList);
        return arrayNode.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // DO NOTHNG
    }
}