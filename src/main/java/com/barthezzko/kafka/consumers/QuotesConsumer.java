package com.barthezzko.kafka.consumers;

import com.barthezzko.kafka.common.TickerInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class QuotesConsumer extends AbstractConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(QuotesConsumer.class);
    private final Map<String, TickerInfo> tickerInfoMap;

    public QuotesConsumer(Map<String, TickerInfo> tickerInfoMap) {
        super(Collections.singletonList("quotes"));
        this.tickerInfoMap = tickerInfoMap;
        logger.info("Initializing consumer with topics:" + getTopics());
    }

    @Override
    void onRecord(ConsumerRecord<String, String> record) {
        Map<String, String> keyValuePairMap = cutToTokens(record);
        if (!keyValuePairMap.containsKey("price") || !keyValuePairMap.containsKey("ticker")){
            throw new IllegalArgumentException("Incoming quote message should contain both price and ticker fields, original message: " + record.value());
        }
        String ticker = keyValuePairMap.get("ticker");
        double quotePrice = Double.valueOf(keyValuePairMap.get("price"));
        tickerInfoMap.get(ticker).appendQuote(quotePrice);
    }

    private Map<String, String> cutToTokens(ConsumerRecord<String, String> record) {
        String raw = record.value();
        String[] pairs = raw.substring(raw.indexOf(":")).split(":");
        return Arrays.stream(pairs)
                .map(pair -> pair.split("="))
                .filter(arr -> arr.length == 2)
                .collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                onRecord(record);
            }
        }
    }
}
