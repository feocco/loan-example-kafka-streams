package com.github.dhoard.kafka.streams.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonNodeArrayListDeserializer<T> implements Deserializer<ArrayList<JsonNode>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonNodeArrayListDeserializer() {
        // DO NOTHNG
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public ArrayList<JsonNode> deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        JsonNode jsonNode = null;

        try {
            jsonNode = OBJECT_MAPPER.readTree(bytes);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        if (jsonNode.size() == 0) {
            return null;
        }

        ArrayList<JsonNode> result = new ArrayList<>();
        ArrayNode arrayNode = (ArrayNode) jsonNode;

        for (int i = 0; i < arrayNode.size(); i++) {
            result.add(arrayNode.get(i));
        }

        return result;
    }

    @Override
    public void close() {
        // DO NOTHNG
    }
}
