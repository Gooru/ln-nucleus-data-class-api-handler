package org.gooru.nucleus.handlers.dataclass.api.processors;

import io.vertx.core.eventbus.Message;

public final class ProcessorBuilder {

  private ProcessorBuilder() {
    throw new AssertionError();
  }

  public static Processor build(Message<Object> message) {
    return new MessageProcessor(message);
  }
}
