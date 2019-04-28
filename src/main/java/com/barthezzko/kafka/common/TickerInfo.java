package com.barthezzko.kafka.common;

public class TickerInfo {

    private double refdataPrice;
    private double lastPrice;
    private int position;
    private double totalPrice;
    private int pricesCollected;

    public static TickerInfo of(double refdataPrice) {
        TickerInfo tickerInfo = new TickerInfo();
        tickerInfo.refdataPrice = refdataPrice;
        return tickerInfo;
    }

    public double getRefdataPrice() {
        return refdataPrice;
    }

    public void setRefdataPrice(double refdataPrice) {
        this.refdataPrice = refdataPrice;
    }

    public int getPosition() {
        return position;
    }

    public void addPosition(int position) {
        this.position += position;
    }

    public synchronized double getAvgPrice() {
        return totalPrice / pricesCollected;
    }

    public synchronized void appendQuote(double quote) {
        totalPrice += quote;
        pricesCollected++;
    }
}
