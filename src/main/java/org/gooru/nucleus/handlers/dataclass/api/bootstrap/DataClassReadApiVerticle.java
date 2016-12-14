package org.gooru.nucleus.handlers.dataclass.api.bootstrap;

import org.gooru.nucleus.handlers.dataclass.api.bootstrap.shutdown.Finalizer;
import org.gooru.nucleus.handlers.dataclass.api.bootstrap.shutdown.Finalizers;
import org.gooru.nucleus.handlers.dataclass.api.bootstrap.startup.Initializer;
import org.gooru.nucleus.handlers.dataclass.api.bootstrap.startup.Initializers;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessagebusEndpoints;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

/**
 * Created by ashish on 9/3/16.
 */
public class DataClassReadApiVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataClassReadApiVerticle.class);

    @Override
    public void start(Future<Void> voidFuture) throws Exception {
        EventBus eb = vertx.eventBus();

        vertx.executeBlocking(blockingFuture -> {
            startApplication();
            blockingFuture.complete();
        }, startApplicationFuture -> {
            if (startApplicationFuture.succeeded()) {
                eb.consumer(MessagebusEndpoints.MBEP_DATACLASS_API, message -> {
                    LOGGER.debug("Received message: " + message.body());
                    vertx.executeBlocking(future -> {
                        MessageResponse result = ProcessorBuilder.build(message).process();
                        future.complete(result);
                    }, res -> {
                        MessageResponse result = (MessageResponse) res.result();
                        message.reply(result.reply(), result.deliveryOptions());
                        JsonObject eventData = result.event();
                        if (eventData != null) {
                            String sessionToken =
                                ((JsonObject) message.body()).getString(MessageConstants.MSG_HEADER_TOKEN);
                            if (sessionToken != null && !sessionToken.isEmpty()) {
                                eventData.put(MessageConstants.MSG_HEADER_TOKEN, sessionToken);
                            } else {
                                LOGGER.warn("Invalid session token received");
                            }
                            eb.send(MessagebusEndpoints.MBEP_EVENT, eventData);
                        }
                    });
                }).completionHandler(result -> {
                    if (result.succeeded()) {
                        voidFuture.complete();
                        LOGGER.info("Data Class Read end point ready to listen");
                    } else {
                        LOGGER.error("Error registering the data class handler. Halting the analytics machinery");
                        voidFuture.fail(result.cause());
                        Runtime.getRuntime().halt(1);
                    }
                });
            } else {
                voidFuture.fail("Not able to initialize the verticle machinery properly");
            }
        });

    }

    @Override
    public void stop() throws Exception {
        shutDownApplication();
        super.stop();
    }

    private void startApplication() {
        Initializers initializers = new Initializers();
        try {
            for (Initializer initializer : initializers) {
                initializer.initializeComponent(vertx, config());
            }
        } catch (IllegalStateException ie) {
            LOGGER.error("Error initializing application", ie);
            Runtime.getRuntime().halt(1);
        }
    }

    private void shutDownApplication() {
        Finalizers finalizers = new Finalizers();
        for (Finalizer finalizer : finalizers) {
            finalizer.finalizeComponent();
        }

    }

}
