package org.gooru.nucleus.handlers.dataclass.api.processors.suggestions;

import java.util.ArrayList;
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
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class SuggestionsPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SuggestionsPerfHandler.class);

  private final ProcessorContext context;
  private String classId;
  private String userId;
  private String scope;
  List<Long> pIds;
  private String collectionType;

  public SuggestionsPerfHandler(ProcessorContext context) {
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
    if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))
        && !StringUtil.isNullOrEmpty(classId)) {
      LOGGER.debug("User ID in session : {} : class : {}", context.userIdFromSession(), classId);
      LazyList<AJEntityClassAuthorizedUsers> owner =
          AJEntityClassAuthorizedUsers.findBySQL(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
              classId, this.context.userIdFromSession());
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

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonArray resultarray = new JsonArray();
    JsonObject resultBody = new JsonObject();
    List<String> userIds = fetchUserId();
    List<Map> activityList = null;
    JsonArray userUsageArray = new JsonArray();
    for (String userId : userIds) {
      JsonObject usageData = new JsonObject();
      if (scope.equals(EventConstants.DAILYCLASSACTIVITY)) {
        if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
          LOGGER.debug("Fetching Performance for Assessments in Class");
          activityList =
              Base.findAll(AJEntityDailyClassActivity.GET_SUGG_PERFORMANCE_FOR_CLASS_ASSESSMENTS,
                  classId, userId, PgUtils.listToPostgresArrayLong(pIds), scope);
        } else {
          activityList =
              Base.findAll(AJEntityDailyClassActivity.GET_SUGG_PERFORMANCE_FOR_CLASS_COLLECTIONS,
                  classId, userId, PgUtils.listToPostgresArrayLong(pIds), scope);
        }
      } else {
        if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
          LOGGER.debug("Fetching Performance for Assessments in Class");
          activityList =
              Base.findAll(AJEntityBaseReports.GET_SUGG_PERFORMANCE_FOR_CLASS_ASSESSMENTS, classId,
                  userId, PgUtils.listToPostgresArrayLong(pIds), scope);
        } else {
          activityList =
              Base.findAll(AJEntityBaseReports.GET_SUGG_PERFORMANCE_FOR_CLASS_COLLECTIONS, classId,
                  userId, PgUtils.listToPostgresArrayLong(pIds), scope);
        }
      }
      if (activityList != null && !activityList.isEmpty()) {
        activityList.forEach(m -> {
          JsonObject contentKpi = new JsonObject();
          contentKpi.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID,
              m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString());
          contentKpi.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE,
              m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE).toString());
          contentKpi.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
              Long.parseLong(m.get(AJEntityDailyClassActivity.ATTR_TIME_SPENT).toString()));
          contentKpi.put(AJEntityDailyClassActivity.ATTR_ATTEMPTS,
              m.get(AJEntityDailyClassActivity.ATTR_ATTEMPTS) != null
                  ? Integer.parseInt(m.get(AJEntityDailyClassActivity.ATTR_ATTEMPTS).toString())
                  : 1);
          contentKpi.put(AJEntityDailyClassActivity.ATTR_PATH_ID,
              Long.valueOf(m.get(AJEntityDailyClassActivity.ATTR_PATH_ID).toString()));
          contentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
          if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
            contentKpi.put(AJEntityDailyClassActivity.ATTR_SCORE,
                m.get(AJEntityDailyClassActivity.ATTR_SCORE) != null
                    ? Math.round(
                        Double.valueOf(m.get(AJEntityDailyClassActivity.ATTR_SCORE).toString()))
                    : null);
            contentKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID,
                m.get(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID).toString());
          } else {
            calculateCollectionScore(userId, m, contentKpi);
            contentKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID,
                AJEntityDailyClassActivity.NA);
          }
          userUsageArray.add(contentKpi);
        });

        usageData.put(JsonConstants.USAGE_DATA, userUsageArray).put(JsonConstants.USERUID, userId);
        resultarray.add(usageData);
      } else {
        LOGGER.debug("No data returned for Suggestion performance");
      }
    }

    resultBody.put(JsonConstants.CONTENT, resultarray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);
  }
  
  @SuppressWarnings("rawtypes")
  private void calculateCollectionScore(String userId, Map m, JsonObject collectionKpi) {
    double maxScore = 0;
    List<Map> collectionMaximumScore = null;
    if (scope.equals(EventConstants.DAILYCLASSACTIVITY)) {
      collectionMaximumScore =
          Base.findAll(AJEntityDailyClassActivity.SELECT_SUGG_PERFORMANCE_COLLECTION_MAX_SCORE, classId, userId,
              Long.valueOf(m.get(AJEntityDailyClassActivity.ATTR_PATH_ID).toString()), scope,
              m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
    } else {
      collectionMaximumScore = Base.findAll(AJEntityBaseReports.SELECT_SUGG_COLLECTION_MAX_SCORE,
          classId, userId, Long.valueOf(m.get(AJEntityDailyClassActivity.ATTR_PATH_ID).toString()), scope,
          m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
    }
    if (collectionMaximumScore != null && !collectionMaximumScore.isEmpty()) {
      for (Map ms : collectionMaximumScore) {
        if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
          maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
        }
      }
    }

    double scoreInPercent = 0;
    Object collectionScore = null;
    if (scope.equals(EventConstants.DAILYCLASSACTIVITY)) {
      collectionScore = Base.firstCell(
          AJEntityDailyClassActivity.GET_SUGG_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE, classId,
          userId, Long.valueOf(m.get(AJEntityDailyClassActivity.ATTR_PATH_ID).toString()), scope,
          m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
    } else {
      collectionScore =
          Base.firstCell(AJEntityBaseReports.GET_SUGG_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE,
              classId, userId, Long.valueOf(m.get(AJEntityDailyClassActivity.ATTR_PATH_ID).toString()), scope,
              m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
    }
    if (collectionScore != null && (maxScore > 0)) {
      scoreInPercent = ((Double.valueOf(collectionScore.toString()) / maxScore) * 100);
      collectionKpi.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
    } else {
      collectionKpi.putNull(AJEntityBaseReports.SCORE);
    }
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
    collectionType = this.context.request().getString(MessageConstants.COLLECTION_TYPE);
  }
  
  private void validateContextRequestFields() {
    if (StringUtil.isNullOrEmpty(scope)
        || !MessageConstants.CM_CA_PROFICIENCY_SOURCE_TYPES.matcher(scope).matches()) {
      LOGGER.warn("Scope is mandatory to fetch Student Suggestion Performance.");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Scope is Missing. Cannot fetch Student Suggestion Performance"));
    }
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory for fetching Student Suggestion Performance");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch Student Suggestion Performance"));
    }
    if (StringUtil.isNullOrEmpty(collectionType)) {
      LOGGER.warn(
          "Collection Type is mandatory to fetch Student Suggestion Performance");
      throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse(
          "Collection Type is Missing. Cannot fetch Student Suggestion Performance"));
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
    if (pIds.size() == 0) {
      LOGGER.warn("PathIds are mandatory to fetch Student Suggestion Performance");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "PathIds are missing or invalid. Cannot fetch Student Suggestion Performance"));
    }
    if (scope.equalsIgnoreCase(MessageConstants.DCA)) {
      scope = EventConstants.DAILYCLASSACTIVITY;
    } else if (scope.equalsIgnoreCase(MessageConstants.PROFICIENCY)) {
      scope = EventConstants.COMPETENCY_MASTERY;
    }
  }
  
  @SuppressWarnings("unchecked")
  private List<String> fetchUserId() {
    List<String> userIds = new ArrayList<>();
    if (!StringUtil.isNullOrEmpty(userId)) {
      userIds.add(userId);
    } else {
      LOGGER.warn(
          "UserID is not in the request to fetch Student Suggestion Perf. Assume user is a teacher");
      if (!StringUtil.isNullOrEmpty(classId)) {
        LazyList<AJEntityCollectionPerformance> usersOfClass = AJEntityCollectionPerformance.findBySQL(
            AJEntityCollectionPerformance.SELECT_DISTINCT_USERID_FOR_CLASS_SUGGESTIONS, classId,
            PgUtils.listToPostgresArrayLong(pIds), scope);
        if (usersOfClass != null) {
          userIds = usersOfClass.collect(AJEntityDailyClassActivity.GOORUUID);
        }
      }
    }
    return userIds;
  }

}
