package org.gooru.nucleus.handlers.dataclass.api.processors;

import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

public interface Processor {
  MessageResponse process();
}
