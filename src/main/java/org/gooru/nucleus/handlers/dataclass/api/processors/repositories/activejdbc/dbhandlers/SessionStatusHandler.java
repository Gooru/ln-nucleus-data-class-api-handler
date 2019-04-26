package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

public class SessionStatusHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionStatusHandler.class);

  private final ProcessorContext context;

  private String sessionId;

  public SessionStatusHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {

    // No Sanity Check required since, no params are being passed in Request Body
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {

    JsonObject resultBody = new JsonObject();
    AJEntityBaseReports baseReport = new AJEntityBaseReports();

    String collectionId = context.collectionId();
    LOGGER.debug("collectionId is " + collectionId);

    this.sessionId = context.sessionId();
    LOGGER.debug("UID is " + this.sessionId);

    List<Map> sessionStatusMap = Base.findAll(AJEntityBaseReports.GET_SESSION_STATUS,
        this.sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);

    if (!sessionStatusMap.isEmpty()) {

      sessionStatusMap.forEach(m -> {
        Integer x = Integer.valueOf(m.get(AJEntityBaseReports.ATTR_COUNT).toString());
        if (x == 0) {
          resultBody.putNull(JsonConstants.CONTENT)
              .put(JsonConstants.MESSAGE,
                  new JsonObject().put(AJEntityBaseReports.SESSION_ID, this.sessionId)
                      .put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS))
              .putNull(JsonConstants.PAGINATE);

        } else {
          resultBody.putNull(JsonConstants.CONTENT)
              .put(JsonConstants.MESSAGE,
                  new JsonObject().put(AJEntityBaseReports.SESSION_ID, this.sessionId)
                      .put(JsonConstants.STATUS, JsonConstants.COMPLETE))
              .putNull(JsonConstants.PAGINATE);
        }
      });

    } else {
      LOGGER.info("Session status cannot be obtained");
    }

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);
  }


  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
