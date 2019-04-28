package com.barthezzko.kafka.producers;

import com.barthezzko.kafka.common.TickerInfo;
import com.barthezzko.kafka.common.Utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QuotesProducer extends AbstractProducer implements Runnable {

    private int randomizationBps;
    private final Random random = new Random();
    private final AtomicInteger quotesCounter = new AtomicInteger(0);
    private final Map<String, TickerInfo> tickerInfoMap;

    public QuotesProducer(Map<String, TickerInfo> tickerInfoMap) {
        super("quotes-producer", "quotes");
        this.tickerInfoMap = tickerInfoMap;
        this.randomizationBps = Integer.valueOf(Utils.getPropertyByName("price.fluctuation"));
    }

    @Override
    public void run() {
        while (true) {
            tickerInfoMap.forEach((ticker, info) -> send(String.format("~%s:ticker=%s:price=%s", quotesCounter.incrementAndGet(), ticker, fluctuate(info.getRefdataPrice()))));
            try {
                TimeUnit.MICROSECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private double fluctuate(double price) {
        double randomizationRatio = 1.00 + (random.nextInt(randomizationBps * 2) - randomizationBps) / 10_000.0;
        logger.debug("rndRatio: " + randomizationRatio);
        return price * randomizationRatio;
    }
}
