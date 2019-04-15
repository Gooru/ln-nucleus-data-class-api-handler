package org.gooru.nucleus.handlers.dataclass.api.constants;

public final class MessagebusEndpoints {
  /*
   * Any change here in end points should be done in the gateway side as well, as both sender and
   * receiver should be in sync
   */

  // Class Reports - Read APIs
  public static final String MBEP_DATACLASS_API = "org.gooru.nucleus.message.bus.dataclassapi";

  public static final String MBEP_EVENT = "org.gooru.nucleus.message.bus.publisher.event";

  public static final String MBEP_AUTH = "org.gooru.nucleus.message.bus.auth";

  private MessagebusEndpoints() {
    throw new AssertionError();
  }
}
