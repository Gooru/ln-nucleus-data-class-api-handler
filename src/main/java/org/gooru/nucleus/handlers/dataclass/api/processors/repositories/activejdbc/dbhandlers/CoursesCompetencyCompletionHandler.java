package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.UUID;

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

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class CoursesCompetencyCompletionHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoursesCompetencyCompletionHandler.class);

  private final ProcessorContext context;

  CoursesCompetencyCompletionHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (StringUtil.isNullOrEmpty(context.getUserIdFromRequest())) {
      LOGGER.error("userId is mandatory to fetch competency completion");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("userId is Missing. Cannot fetch competency completion"),
              ExecutionStatus.FAILED);
    }
    if (!this.context.request().containsKey(EventConstants.COURSE_IDS) || this.context.request().getJsonArray(EventConstants.COURSE_IDS) == null
            || this.context.request().getJsonArray(EventConstants.COURSE_IDS).isEmpty()) {
      LOGGER.error("course ids are mandatory to fetch competency completion");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("courseIds are missing. Cannot fetch competency completion"),
              ExecutionStatus.FAILED);
    }
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if ((!context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      LOGGER.debug("validateRequest() FAILED");
      return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a valid user"), ExecutionStatus.FAILED);
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonArray courseIds = this.context.request().getJsonArray(EventConstants.COURSE_IDS);
    JsonArray resultArray = new JsonArray();
    JsonObject result = new JsonObject();

    LOGGER.debug("courseIds : {}", courseIds);
    LOGGER.debug("userId : {}", context.getUserIdFromRequest());

    courseIds.stream().forEach(courseId -> {
      if (validateCourseId(courseId.toString())) {
        Object completedCount = Base.firstCell(AJEntityBaseReports.COURSE_COMPETENCY_COMPLETION_COUNT, courseId, context.getUserIdFromRequest());
        JsonObject completionData = new JsonObject();
        LOGGER.debug("Course ID : {} ", courseId);
        Object totalCount = Base.firstCell(AJEntityBaseReports.COURSE_COMPETENCY_TOTAL_COUNT, courseId);
        completionData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount == null ? 0 : totalCount);
        completionData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, completedCount == null ? 0 : completedCount);
        LOGGER.debug("totalCount {} - CompletedCount : {} ", totalCount, completedCount);
        completionData.put(AJEntityBaseReports.ATTR_COURSE_ID, courseId);
        resultArray.add(completionData);
      } else {
        LOGGER.warn("Invalid courseId -> " + courseId + " Simply Ignore now.");
      }
    });
    result.put(JsonConstants.USAGE_DATA, resultArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

  private boolean validateCourseId(String id) {
    return !(id == null || id.isEmpty()) && validateUuid(id);
  }

  private boolean validateUuid(String uuidString) {
    try {
      UUID uuid = UUID.fromString(uuidString);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
