package org.example.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(BankTransactionsProducer.class);
        Properties producerConfigs = new Properties();

        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.ACKS_CONFIG,"all");
        producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG,Integer.toString(10000));

        Producer<String, String> producer = new KafkaProducer<String, String>(producerConfigs);

        logger.info("hello");

        //start producing
        int i=1;
        while(true)
        {
            System.out.printf("Producing Batch %d\n" , i);
            try
            {
                producer.send(generateNewTransaction("Sabarish-1"));
                Thread.sleep(100);
                producer.send(generateNewTransaction("Sabarish-2"));
                Thread.sleep(100);
                producer.send(generateNewTransaction("Sabarish-3"));
                Thread.sleep(100);
                producer.send(generateNewTransaction("Sabarish-4"));
                Thread.sleep(100);
                i++;

            } catch (InterruptedException e) {
                break;
            }

        }
        producer.flush();
    }

    private static ProducerRecord<String, String> generateNewTransaction(String name) {
        String topicName = "Bank-Transactions";

        var transactions = JsonNodeFactory.instance.objectNode();

        Integer amount = ThreadLocalRandom.current().nextInt(0,100);

        //get date
        Instant instant = Instant.now();
        transactions.put("name",name);
        transactions.put("amount", amount);
        transactions.put("timestamp", instant.toString());

        return new ProducerRecord<>(topicName,name,transactions.toString());
        
    }
}
