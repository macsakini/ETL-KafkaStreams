package org.buysa;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        Properties localproperties = new Properties();
        try (InputStream propertiesfile = ClassLoader.getSystemResourceAsStream("local.properties");) {
            localproperties.load(propertiesfile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, localproperties.getProperty("CLIENT_ID"));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,localproperties.getProperty("BOOTSTRAP_SERVERS") );
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 500000000);

        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        if(localproperties.getProperty("PRODUCTION").equals("true")){
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, localproperties.getProperty("SSL_TRUSTSTORE_PROD_LOCATION"));
            properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, localproperties.getProperty("SSL_KEYSTORE_PROD_LOCATION"));
        }else{
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, localproperties.getProperty("SSL_TRUSTSTORE_DEV_LOCATION"));
            properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, localproperties.getProperty("SSL_KEYSTORE_DEV_LOCATION"));
        }
        properties.setProperty(SslConfigs.SSL_PROTOCOL_CONFIG, "TLS");
        properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, localproperties.getProperty("SSL_PASSWORD"));
        properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,localproperties.getProperty("SSL_PASSWORD"));
        properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, localproperties.getProperty("SSL_PASSWORD"));
        properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");


        StreamsBuilder builder = new StreamsBuilder();

        KStream <String, String> inputStream = builder.stream("items");

        KGroupedStream<String, String> inputGrouped = inputStream
                .mapValues((value) -> {
                    JSONObject object =  new JSONObject(value);
                    return value;
                })
                .groupByKey();

        Materialized.as("eventstore");

        KTable<String, String> inputTable = inputGrouped.reduce(
                (event1, event2) -> event1 + " , " + event2,
                Materialized.with(Serdes.String(), Serdes.String())
        );

        try{
            inputTable.toStream()
                    .peek((key, value) -> {
                        System.out.println(value);
                    }).to("processeditems", Produced.with(Serdes.String(),Serdes.String()));
        } catch(Exception ex){
            System.out.println("Error occured due to " + ex);
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        try{
            streams.start();
        }catch(AuthorizationException e){
            e.printStackTrace();
        }finally{
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }


    }
}