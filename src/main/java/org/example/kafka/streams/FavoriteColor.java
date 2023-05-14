package org.example.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Predicate;
//https://github.com/simplesteph/kafka-streams-course
public class FavoriteColor {

    public static void main(String[] args)
    {
        Properties configs = new Properties();
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        String inputTopic = "FavoriteColorStream";
        StreamsBuilder builder = new StreamsBuilder();

        // 1. Read stream from topic
        KStream<String, String> stream = builder.stream(inputTopic);
            // null: sabarish,red
            //null: sabarish,green


       var usersAndColors =
               // 2. filter bad records
            stream.filter((key,value) -> value.contains(","))
                    // 3. map keys using userName
                    .selectKey((key, value) -> value.split(",")[0].toLowerCase()) // repartitions
                    // 4. map colors in lowercase
                    .mapValues(value -> value.split(",")[1].toLowerCase())
                    // 5. filter colors
                    .filter((user,color)-> Arrays.asList("green","red", "blue").contains(color));

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

       String IntermediateTopic = "User_Color";
        String outputTopic = "ColorCount";

        // 6 . write to intermediate topic
        usersAndColors.to(IntermediateTopic);
        // 7. read from topic as table -> so it gets snapshot
        KTable<String, String> KTable = builder.table(IntermediateTopic);
        System.out.println(KTable);


        // 8. apply groupBy color
       var countByColorsTable =  KTable.groupBy((user, color) -> new KeyValue<>(color, color))
                // 9 . count ()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColors")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
               ;

        countByColorsTable.toStream().to(outputTopic,Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), configs);
        kafkaStreams.cleanUp();
        var state = kafkaStreams.state();

        System.out.println(state);

        kafkaStreams.start();

        // print topolpgy
        kafkaStreams.localThreadsMetadata().forEach(System.out::println);

        //Runtime shutdown hooks
        Runtime.getRuntime().addShutdownHook(new Thread(
                kafkaStreams::close
        ));



    }

}
