// https://github.com/jbfletch/kstreams-des-demo/blob/master/src/main/java/org/happypants/demo/des/serde/JsonSerde.java
package org.happypants.demo.des.serde;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde implements Serde<JsonNode> {


    @Override
    public void configure(java.util.Map<java.lang.String, ?> configs, boolean isKey) {
    }

    @Override
    public Serializer<JsonNode> serializer() {
        return new JsonSerializer();
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return new JsonDeserializer();
    }

    public JsonSerde() {
    }
}