package io.confluent.developer.basic;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BasicStreams {

    public static void main(String[] args) throws IOException {
        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");
        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.topic");
        final String orderNumbersStart = "orderNumber-";
        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        firstStream.peek((k, v) -> System.out.println("Incoming record - key " + k + " value " + v))
                .filter((k, v) -> v.contains(orderNumbersStart))
                .mapValues(v -> v.substring(v.indexOf("-") + 1))
                .filter((k, v) -> Long.parseLong(v) > 1000)
                .peek((k, v) -> System.out.println("Outgoing record - key " + k + " value " + v))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        //Unfortunately, this doesn't seem to be exiting cleanly... Not sure of what's missing yet.
        try (KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                streams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
            System.exit(0);
        }
    }
}

