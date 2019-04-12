package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.formatter;

import java.util.List;

/**
 * Created by ashish on 20/1/16.
 */
public final class JsonFormatterBuilder {

  private JsonFormatterBuilder() {
    throw new AssertionError();
  }

  public static JsonFormatter buildSimpleJsonFormatter(boolean pretty, List<String> attributes) {

    return new SimpleJsonFormatter(pretty, attributes);
  }
}
