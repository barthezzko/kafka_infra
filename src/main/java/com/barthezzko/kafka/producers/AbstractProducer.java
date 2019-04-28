package com.barthezzko.kafka.producers;

import com.barthezzko.kafka.common.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class AbstractProducer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractProducer.class);
    private static final String  PRODUCER_PREFIX = "producer.";
    private final Producer<String, String> producer;
    private final String topic;
    protected final String name;
    private AtomicInteger messageCounter = new AtomicInteger(0);

    public AbstractProducer(String name, String topic) {
        this.topic = topic;
        this.name = name;
        Properties producerProperties = Utils.getPropertiesByPrefix(PRODUCER_PREFIX);
        producer = new KafkaProducer<>(producerProperties);
    }

    public void send(String message){
        producer.send(new ProducerRecord<>(topic, name + messageCounter.incrementAndGet(), message));
    }


    @Deprecated
    public void stop(){
        producer.close();
    }
}
