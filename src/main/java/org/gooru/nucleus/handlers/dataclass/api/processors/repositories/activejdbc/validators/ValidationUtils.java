package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.validators;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValidationUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationUtils.class);
  private ValidationUtils() {
    throw new AssertionError();
  }

  public static boolean isNullOrEmpty(String value) {
    return (value == null || value.isEmpty() || value.trim().isEmpty());
  }

  public static boolean isValidUUID(String id) {
    try {
      if (!isNullOrEmpty(id) && id.length() == 36) {
        UUID.fromString(id);
        return true;
      }

      return false;
    } catch (IllegalArgumentException iae) {
      LOGGER.warn("Invalid UUID string '{}'", id);
    }

    return false;
  }

}
