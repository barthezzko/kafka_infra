package com.barthezzko.kafka.consumers;

import com.barthezzko.kafka.common.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class AbstractConsumer {

    protected Logger logger = LoggerFactory.getLogger(AbstractConsumer.class);
    private final List<String> topics;
    private volatile boolean isEnabled;
    private final static String CONSUMER_PREFIX = "consumer.";

    public AbstractConsumer(List<String> topics){
        this.topics = topics;
        Properties consumerProperties = Utils.getPropertiesByPrefix(CONSUMER_PREFIX);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(topics);
        while (isEnabled) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                onRecord(record);
            }
        }
    }

    protected List<String> getTopics(){
        return Collections.unmodifiableList(topics);
    }

    abstract void onRecord(ConsumerRecord<String, String> record);

    public void stop(){
        isEnabled = false;
    }
}
