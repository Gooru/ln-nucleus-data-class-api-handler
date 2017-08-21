package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonObject;

public class CourseCompetencyCompletionHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CourseCompetencyCompletionHandler.class);

  private final ProcessorContext context;

  CourseCompetencyCompletionHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
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
    JsonObject result = new JsonObject();

    LOGGER.debug("courseId : {}", context.courseId());
    LOGGER.debug("userId : {}", context.getUserIdFromRequest());

    if (StringUtil.isNullOrEmpty(context.getUserIdFromRequest())) {
      LOGGER.warn("userId is mandatory to fetch course competency completion");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("userId is Missing. Cannot fetch course competency completion"),
              ExecutionStatus.FAILED);
    }

    result.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
    result.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }
}
