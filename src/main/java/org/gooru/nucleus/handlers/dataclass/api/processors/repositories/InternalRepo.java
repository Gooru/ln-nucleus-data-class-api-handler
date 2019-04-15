
package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * @author szgooru Created On 12-Apr-2019
 */
public interface InternalRepo {

  MessageResponse getAllClassPerformance();

  MessageResponse getClassDCAPerformance();
}
