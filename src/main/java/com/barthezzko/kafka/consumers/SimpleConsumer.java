package com.barthezzko.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SimpleConsumer extends AbstractConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    public SimpleConsumer(){
        super(Arrays.asList("streams-pipe-output"));
        logger.info("Initializing consumer with topics:" + getTopics());
    }

    public static void main(String[] args) {
        new SimpleConsumer();
    }

    @Override
    void onRecord(ConsumerRecord<String, String> record) {
        logger.info(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
    }
}
