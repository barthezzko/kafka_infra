package com.barthezzko.kafka.web;

import com.barthezzko.kafka.common.Utils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerRunner {

    private final static Logger logger = LoggerFactory.getLogger(ServerRunner.class);

    public static void main(String[] args) {
        Server server = new Server();
        server.start();
    }

    static class Server extends AbstractVerticle {
        public void start() {
            int webServerPort = Integer.valueOf(Utils.getPropertyByName("web.server.port"));
            logger.info("running server at port: " + webServerPort);

            Vertx vertx = Vertx.vertx();
            Router router = Router.router(vertx);
            router.route("/").handler(ctx -> {
                HttpServerResponse response = ctx.response();
                response
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(Utils.getConfigMap()));
            });

            vertx
                    .createHttpServer()
                    .requestHandler(router::accept)
                    .listen(webServerPort);
        }
    }
}
