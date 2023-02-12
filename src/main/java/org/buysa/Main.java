package org.buysa;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "BuySAKafkaStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "13.246.19.88:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream <String, JSONObject> inputStream = builder.stream("newitemstest");

        inputStream.foreach((key, value) -> {
            JSONObject json = new JSONObject(value);
            System.out.println(json.toString());
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}