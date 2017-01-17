package org.gooru.nucleus.handlers.dataclass.api.bootstrap;

import org.gooru.nucleus.handlers.dataclass.api.constants.MessagebusEndpoints;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;

/**
 * @author Insights Team
 */
public class DataClassReadApiVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataClassReadApiVerticle.class);

  @Override
  public void start(Future<Void> voidFuture) throws Exception {

    final EventBus eb = vertx.eventBus();
    eb.consumer(MessagebusEndpoints.MBEP_DATACLASS_API, message -> {
      LOGGER.debug("Received message: {}", message.body());
      vertx.executeBlocking(future -> {
        MessageResponse result = ProcessorBuilder.build(message).process();
        future.complete(result);
      }, res -> {
        MessageResponse result = (MessageResponse) res.result();
        message.reply(result.reply(), result.deliveryOptions());      
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
  }

  @Override
  public void stop(Future<Void> voidFuture) throws Exception {
    voidFuture.complete();
  }
}
