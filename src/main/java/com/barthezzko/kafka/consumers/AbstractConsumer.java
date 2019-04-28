package com.barthezzko.kafka.consumers;

import com.barthezzko.kafka.common.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class AbstractConsumer {

    protected Logger logger = LoggerFactory.getLogger(AbstractConsumer.class);
    private final List<String> topics;
    private final static String CONSUMER_PREFIX = "consumer.";
    protected KafkaConsumer<String, String> consumer;

    public AbstractConsumer(List<String> topics){
        this.topics = topics;
        Properties consumerProperties = Utils.getPropertiesByPrefixAndStripOffPrefix(CONSUMER_PREFIX);
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(topics);
    }

    protected List<String> getTopics(){
        return Collections.unmodifiableList(topics);
    }

    abstract void onRecord(ConsumerRecord<String, String> record);
}
