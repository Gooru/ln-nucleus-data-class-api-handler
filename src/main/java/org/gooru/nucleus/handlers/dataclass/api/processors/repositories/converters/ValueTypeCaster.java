package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters;

import java.math.BigDecimal;

public class ValueTypeCaster {

  public static Object castValue(Object val) {
    /**
     * Other data types to be checked..
     */
    if (val instanceof BigDecimal) {
      return ((Number) val).longValue();
    }
    return val.toString();
  }
}
