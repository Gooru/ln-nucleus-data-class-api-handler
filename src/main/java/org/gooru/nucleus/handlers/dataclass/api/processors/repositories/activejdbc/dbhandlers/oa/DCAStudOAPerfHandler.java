package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.utils.PgUtils;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCAStudOAPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAStudOAPerfHandler.class);

  private final ProcessorContext context;
  private String classId;
  private String userId;
  private JsonArray dcaContentIds;
  private String collectionType;

  public DCAStudOAPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    try {
      validateContextRequest();
      userId = this.context.request().getString(MessageConstants.USER_ID);
      collectionType = this.context.request().getString(MessageConstants.COLLECTION_TYPE);
      dcaContentIds = this.context.request().getJsonArray(MessageConstants.DCA_CONTENT_IDS);
      classId = this.context.request().getString(MessageConstants.CLASS_ID);
      validateContextRequestFields();

    } catch (MessageResponseWrapperException mrwe) {
      return new ExecutionResult<>(mrwe.getMessageResponse(),
          ExecutionResult.ExecutionStatus.FAILED);
    }
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          this.context.classId(), this.context.userIdFromSession());
      if (owner.isEmpty()) {
        LOGGER.debug("validateRequest() FAILED");
        return new ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
      }
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject resultBody = new JsonObject();
    JsonArray userUsageArray = new JsonArray();
    LOGGER.debug("userId : {} - dcaContentIds:{}", userId, dcaContentIds);
    List<Long> collIds = new ArrayList<>(dcaContentIds.size());
    for (Object collId : dcaContentIds) {
      try {
        collIds.add(Long.valueOf(collId.toString()));
      } catch (NumberFormatException nfe) {
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse(
                "NumberFormatException:Invalid dcaContentIds provided to fetch Student Performance in OAs"),
            ExecutionStatus.FAILED);
      }
    }
    List<String> userIds = fetchUserId();

    for (String userId : userIds) {
      JsonArray activityArray = new JsonArray();
      JsonObject dateActivity = new JsonObject();

      List<Map> activityList = null;
        LOGGER.debug("Fetching Performance for OA in Class");
        activityList =
            Base.findAll(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_OAS, classId,
                PgUtils.listToPostgresArrayLong(collIds), userId,
                AJEntityDailyClassActivity.ATTR_CP_EVENTNAME);
      if (activityList != null && !activityList.isEmpty()) {
        generateActivityData(activityArray, activityList);
      } else {
        LOGGER.debug("No data available for ANY of the dcaContentIds passed on to this endpoint");
      }
      dateActivity.put(JsonConstants.ACTIVITY, activityArray);
      dateActivity.put(JsonConstants.USERID, userId);
      userUsageArray.add(dateActivity);
    }

    resultBody.put(JsonConstants.USAGE_DATA, userUsageArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);

  }

  @SuppressWarnings("rawtypes")
  private void generateActivityData(JsonArray activityArray, List<Map> activityList) {
    activityList.forEach(m -> {
      JsonObject contentKpi = new JsonObject();
      contentKpi.put(AJEntityDailyClassActivity.ATTR_DCA_CONTENT_ID,
          Integer.valueOf(m.get(AJEntityDailyClassActivity.ATTR_DCA_CONTENT_ID).toString()));
      contentKpi.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID,
          m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString());
      contentKpi.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
          Long.parseLong(m.get(AJEntityDailyClassActivity.ATTR_TIME_SPENT).toString()));
      contentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
        contentKpi.put(AJEntityDailyClassActivity.ATTR_SCORE, m.get(AJEntityDailyClassActivity.ATTR_SCORE) != null ?
            Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())) : null);        
        contentKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID,
            m.get(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID).toString());
      activityArray.add(contentKpi);
    });
  }

  @SuppressWarnings("unchecked")
  private List<String> fetchUserId() {
    List<String> userIds;
    String addCollTypeFilterToQuery = " AND collection_type = 'offline-activity'";
    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn(
          "UserID is not in the request to fetch Student Performance in Course. Assume user is a teacher");
      LazyList<AJEntityDailyClassActivity> userIdOfClass = AJEntityDailyClassActivity
          .findBySQL(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_DAILY_CLASS_ACTIVITY
              + addCollTypeFilterToQuery, this.classId);
      userIds = userIdOfClass.collect(AJEntityDailyClassActivity.GOORUUID);
    } else {
      userIds = new ArrayList<>(1);
      userIds.add(userId);
    }
    return userIds;
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

  private void validateContextRequest() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Performance in OAs");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Student Performance in OAs"));
    }
  }

  private void validateContextRequestFields() {
    if (StringUtil.isNullOrEmpty(collectionType)) {
      LOGGER.warn(
          "Collection Type is mandatory to fetch Student Performance in OAs.");
      throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse(
          "Collection Type is Missing. Cannot fetch Student Performance in OAs"));
    }
    if (dcaContentIds == null || dcaContentIds.isEmpty()) {
      LOGGER.warn("DcaContentIds are mandatory to fetch Student Performance in OAs");
      throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse(
          "DcaContentIds are Missing. Cannot fetch Student Performance for OA"));
    }
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory for fetching Student Performance in Class Activity");
      throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse(
          "Class Id Missing. Cannot fetch Student Performance in Class Activity"));
    }
  }
}
