package org.example.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

public class Main {

    public Topology createTopology()
    {

        Properties configs = new Properties();

        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        String topic = "Word-Count-Input";
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> kStream = streamsBuilder.stream(topic);

        //1. convert to lowercase
        var kTable=       kStream
                .mapValues(topicContent-> topicContent.toLowerCase())

                //2. flat map values
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                //3. selectbyKey
                .selectKey((key,value)-> value)
                //4. count
                .groupByKey().count(Materialized.<String,Long, KeyValueStore<Bytes, byte[]>>as("count"));

        // converting KTable back to Stream using .toStream and is finally written topic using .to()
        kTable.toStream().to("Word-Count-Output", Produced.with(Serdes.String(), Serdes.Long()));

        return  streamsBuilder.build();


    }
    public static void main(String[] args) {

    }
}