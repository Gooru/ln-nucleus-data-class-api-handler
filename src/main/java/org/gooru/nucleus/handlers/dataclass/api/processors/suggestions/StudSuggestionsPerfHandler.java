package org.gooru.nucleus.handlers.dataclass.api.processors.suggestions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollectionPerformance;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.utils.PgUtils;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class StudSuggestionsPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudSuggestionsPerfHandler.class);

  private final ProcessorContext context;
  private String classId;
  private String userId;
  private String scope;
  List<Long> pIds;

  public StudSuggestionsPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    try {
      validateContextRequest();
      initializeRequestParams();
      validateContextRequestFields();

    } catch (MessageResponseWrapperException mrwe) {
      return new ExecutionResult<>(mrwe.getMessageResponse(),
          ExecutionResult.ExecutionStatus.FAILED);
    }
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
    JsonArray userUsageArray = new JsonArray();
    LOGGER.debug("userId : {} - pathIds:{}", userId, pIds);
    List<AJEntityCollectionPerformance> items = null;
    if (pIds.size() > 0) {
      if (classId != null) {
        items = AJEntityCollectionPerformance.findBySQL(
            AJEntityCollectionPerformance.FETCH_SUGG_ITEM_PERFORMANCE_IN_CLASS, classId, userId,
            PgUtils.listToPostgresArrayLong(pIds), scope);
      } else {
        items = AJEntityCollectionPerformance.findBySQL(
            AJEntityCollectionPerformance.FETCH_ALL_SUGG_ITEM_PERFORMANCE, userId,
            PgUtils.listToPostgresArrayLong(pIds), scope);
      }
      if (!items.isEmpty()) {
        userUsageArray =
            SuggestionsPerformanceResponseBuilder.buildSuggestionPerformanceResponse(items).build();

        // Collection_perf table to also capture caid, until this support is added, below code can
        // help return caid for each dca session in response.
        if (scope.equalsIgnoreCase(EventConstants.DCA)) {
          enrichResponseWithCaId(userUsageArray);
        }
      }
    } else {
      LOGGER.debug("No data returned for Suggestion performance");
    }
    
    resultBody.put(JsonConstants.USAGE_DATA, userUsageArray).put(JsonConstants.USERUID, userId);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);
  }

  private void enrichResponseWithCaId(JsonArray userUsageArray) {
    List<String> caSessionIds = new ArrayList<>();
    userUsageArray.forEach(i -> {
      JsonObject item = (JsonObject) i;
      caSessionIds.add(item.getString(AJEntityBaseReports.ATTR_SESSION_ID));
    });

    Map<String, Long> caIdMap = fetchDcaContentIdOfGivenSessions(caSessionIds);
    userUsageArray.forEach(i -> {
      JsonObject item = (JsonObject) i;
      item.put(AJEntityDailyClassActivity.DCA_CONTENT_ID,
          caIdMap.get(item.getString(AJEntityBaseReports.ATTR_SESSION_ID)));
    });
  }
  
  private Map<String, Long> fetchDcaContentIdOfGivenSessions(List<String> caSessionIds) {
    Map<String, Long> contents = null;
    List<AJEntityDailyClassActivity> contentModels =
        AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.GET_DCA_IDS_FOR_SESSIONS,
            PgUtils.listToPostgresArrayString(caSessionIds));
    if (contentModels != null) {
      contents = new HashMap<>();
      for (AJEntityDailyClassActivity contentModel : contentModels) {
        contents.put(contentModel.getString(AJEntityDailyClassActivity.SESSION_ID),
            contentModel.getLong(AJEntityDailyClassActivity.DCA_CONTENT_ID));
      }
    }
    return contents;
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

  private void validateContextRequest() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Suggestion Performance");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Student Suggestion Performance"));
    }
  }

  private void initializeRequestParams() {
    userId = this.context.request().getString(MessageConstants.USER_ID);
    scope = this.context.request().getString(MessageConstants.SOURCE);
    classId = this.context.request().getString(MessageConstants.CLASS_ID);
  }
  
  private void validateContextRequestFields() {
    if (StringUtil.isNullOrEmpty(scope)
        || !MessageConstants.CM_CA_PROFICIENCY_SOURCE_TYPES.matcher(scope).matches()) {
      LOGGER.warn("Scope is mandatory to fetch Student Suggestion Performance.");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Scope is Missing. Cannot fetch Student Suggestion Performance"));
    }
    if (MessageConstants.CM_CA_SOURCE_TYPES.matcher(scope).matches()
        && StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory for fetching Student Suggestion Performance");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch Student Suggestion Performance"));
    }
    JsonArray pathIds = this.context.request().getJsonArray(MessageConstants.PATH_IDS);
    if (pathIds == null || pathIds.isEmpty()) {
      LOGGER.warn("PathIds are mandatory to fetch Student Suggestion Performance");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "PathIds are Missing. Cannot fetch Student Suggestion Performance"));
    }
    pIds = new ArrayList<>(pathIds.size());
    for (Object pathId : pathIds) {
      try {
        if (pathId != null) {
          Long path = Long.valueOf(pathId.toString());
          if (path > 0) {
            pIds.add(path);
          }
        }
      } catch (NumberFormatException nfe) {
        throw new MessageResponseWrapperException(
            MessageResponseFactory.createInvalidRequestResponse(
                "NumberFormatException:Invalid pathIds provided to fetch Student Suggestion in class"));
      }
    }
    if (scope.equalsIgnoreCase("dca")) {
      scope = EventConstants.DCA;
    }
  }
}
