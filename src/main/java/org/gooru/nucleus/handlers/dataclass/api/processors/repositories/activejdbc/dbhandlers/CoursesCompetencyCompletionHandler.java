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

    JsonArray classIds = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
    JsonArray resultArray = new JsonArray();
    JsonObject result = new JsonObject();

    LOGGER.debug("classIds : {}", classIds);
    LOGGER.debug("userId : {}", context.getUserIdFromRequest());

    if (StringUtil.isNullOrEmpty(context.getUserIdFromRequest())) {
      LOGGER.warn("userId is mandatory to fetch course competency completion");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("userId is Missing. Cannot fetch course competency completion"),
              ExecutionStatus.FAILED);
    }

    classIds.stream().forEach(classId -> {
      List<Map> completedMapCount = Base.findAll(AJEntityBaseReports.COURSE_COMPETENCY_COMPLETION_COUNT, classId, context.getUserIdFromRequest());
      JsonObject completionData = new JsonObject();
      if (!completedMapCount.isEmpty()) {
        completedMapCount.stream().forEach(completion -> {
          Object totalCount = Base.firstCell(AJEntityBaseReports.COURSE_COMPETENCY_TOTAL_COUNT, completion.get(AJEntityBaseReports.COURSE_GOORU_OID));
          completionData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount == null ? 0 : totalCount);
          completionData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                  completion.get("completedCount") == null ? 0 : completion.get("completedCount"));
          LOGGER.debug("totalCount {} - CompletedCount : {} ", totalCount, completion.get("completedCount"));
        });

      } else {
        completionData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
        completionData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);
      }
      completionData.put(AJEntityBaseReports.ATTR_CLASS_ID, classId);
      resultArray.add(completionData);
    });
    result.put(JsonConstants.USAGE_DATA, resultArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }
}
