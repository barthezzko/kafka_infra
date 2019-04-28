package com.barthezzko.kafka.producers;

import com.barthezzko.kafka.common.Utils;

public class QuotesProducer extends AbstractProducer {

    public QuotesProducer() {

        super("quotes-producer", Utils.getPropertyByName("consumer.trades"));
    }
}
