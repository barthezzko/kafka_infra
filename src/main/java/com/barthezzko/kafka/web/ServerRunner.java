package com.barthezzko.kafka.web;

import com.barthezzko.kafka.common.TickerInfo;
import com.barthezzko.kafka.common.Utils;
import com.barthezzko.kafka.consumers.QuotesConsumer;
import com.barthezzko.kafka.producers.QuotesProducer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerRunner {

    private final static Logger logger = LoggerFactory.getLogger(ServerRunner.class);
    private final static ExecutorService executorService = Executors.newFixedThreadPool(2);
    private final static Map<String, TickerInfo> tickerInfoMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Utils.getRefdata().forEach((ticker, price)->{
            tickerInfoMap.put(ticker, TickerInfo.of(price));
        });
        Server server = new Server();
        server.start();
    }

    static class Server extends AbstractVerticle {
        private static final String JSON_MIME_TYPE = "application/json";

        public void start() {
            int webServerPort = Integer.valueOf(Utils.getPropertyByName("web.server.port"));
            logger.info("running server at port: " + webServerPort);

            executorService.submit(new QuotesProducer(tickerInfoMap));
            executorService.submit(new QuotesConsumer(tickerInfoMap));

            Vertx vertx = Vertx.vertx();
            Router router = Router.router(vertx);
            router.route("/config").handler(ctx -> ctx.response()
                    .putHeader("content-type", JSON_MIME_TYPE)
                    .end(Json.encode(Utils.getConfigMap())));

            router.route("/").handler(ctx -> {
                ctx.response()
                        .putHeader("content-type", JSON_MIME_TYPE)
                        .end(Json.encode(Utils.getConfigMap()));
            });

            router.route("/refdata").handler(ctx -> {
                ctx.response()
                        .putHeader("content-type", JSON_MIME_TYPE)
                        .end(Json.encode(Utils.getRefdata()));
            });

            router.route("/refdataSnapshot").handler(ctx -> {
                ctx.response()
                        .putHeader("content-type", JSON_MIME_TYPE)
                        .end(Json.encode(tickerInfoMap));
            });
            vertx
                    .createHttpServer()
                    .requestHandler(router::accept)
                    .listen(webServerPort);
        }
    }
}
