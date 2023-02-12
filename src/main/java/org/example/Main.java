package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONObject;
import java.lang.Math;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        // Set up the configuration for the Kafka Streams application
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-json-example");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "13.246.19.88:9093");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Build the Kafka Streams processing pipeline
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("items");

        KGroupedStream<String, String> groupedStream = inputStream.groupByKey();

        KTable<String, String> aggStream = groupedStream.reduce((aggValue, keyvalue) -> aggValue+keyvalue);

        aggStream.toStream().peek((key, value) -> System.out.println(key));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        // Add a shutdown hook to properly close the application when it is interrupted
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
