package ru.boro.kfkstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import ru.boro.kfkstream.utils.Utils;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class App {

    public static void main(String[] args) throws Exception {

        var builder = new StreamsBuilder();

        Serde<String> strSerde = Serdes.String();

        var interval = Duration.ofMinutes(5);

        KTable<Windowed<String>, Long> eventCountTable = builder
                .stream("topic-events", Consumed.with(strSerde, strSerde))
                .groupByKey()
                .windowedBy(TimeWindows.of(interval)).count();

        eventCountTable.toStream().foreach((key, val) -> System.out.println(key + " : " + val));

        KStream<String, String> countStream = eventCountTable.toStream().map((window, val) -> {
            String key = window.key();
            return KeyValue.pair(key, val.toString());
        });
        countStream.to("topic_results", Produced.with(Serdes.String(), Serdes.String()) );

        runStockApp(builder, "ex8",
                b -> {
                    b.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
                });
    }

    public static void runStockApp(StreamsBuilder builder, String name,
                                   Consumer<Map<String, Object>> configBuilder) throws Exception {

        var topology = builder.build();

        try (
                var kafkaStreams = new KafkaStreams(topology, Utils.createStreamsConfig(b -> {
                    b.put(StreamsConfig.APPLICATION_ID_CONFIG, name + "-" + UUID.randomUUID().hashCode());
                    b.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

                    configBuilder.accept(b);
                }));
        ) {
            kafkaStreams.start();

            Thread.sleep(20000);
        }
    }
}
