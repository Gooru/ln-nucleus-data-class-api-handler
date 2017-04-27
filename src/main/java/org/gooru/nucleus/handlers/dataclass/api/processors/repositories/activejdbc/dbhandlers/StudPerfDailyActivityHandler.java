package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

public class StudPerfDailyActivityHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfDailyActivityHandler.class);
  private static final String REQUEST_USERID = "userId";
  private static final String START_DATE = "startDate";
  private static final String END_DATE = "endDate";
  private static final String REQUEST_COLLECTION_TYPE = "collectionType";

  private final ProcessorContext context;
  private String userId;
  private String classId;
  private String collectionType;
  private JsonArray collectionIds;
  private long questionCount;

  /*
   * JsonArray assessmentArray = new JsonArray();
   */
  public StudPerfDailyActivityHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Performance in Assessments");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessments"),
              ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() == null
            || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
      if (owner.isEmpty()) {
        LOGGER.debug("validateRequest() FAILED");
        return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
      }
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    JsonObject resultBody = new JsonObject();
    JsonArray userUsageArray = new JsonArray();
    this.userId = this.context.request().getString(REQUEST_USERID);
    String sDate = this.context.request().getString(START_DATE);
    String eDate = this.context.request().getString(END_DATE);

    if (StringUtil.isNullOrEmpty(eDate) || StringUtil.isNullOrEmpty(sDate)) {
      LOGGER.warn("Start Date and End Date are mandatory to fetch Student Performance in Daily Class Activity.");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
              "Start Date and End Date are Missing. Cannot fetch Student Performance in Daily Class Activity"), ExecutionStatus.FAILED);

    }
    Date startDate = Date.valueOf(sDate);
    Date endDate = Date.valueOf(eDate);

    this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
    if (StringUtil.isNullOrEmpty(this.collectionType)) {
      LOGGER.warn("Collection Type is mandatory to fetch Student Performance in Daily Class Activity.");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
              "Collection Type is Missing. Cannot fetch Student Performance in Daily Class Activity"), ExecutionStatus.FAILED);

    }
    this.collectionIds = this.context.request().getJsonArray(MessageConstants.COLLECTION_IDS);
    LOGGER.debug("userId : {} - collectionIds:{}", userId, this.collectionIds);

    if (collectionIds.isEmpty()) {
      LOGGER.warn("CollectionIds are mandatory to fetch Student Performance in Assessments");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("CollectionIds are Missing. Cannot fetch Student Performance for Assessments"),
              ExecutionStatus.FAILED);
    }

    List<String> collIds = new ArrayList<>();
    for (Object s : this.collectionIds) {
      collIds.add(s.toString());
    }

    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);

    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory for fetching Student Performance in Daily Class Activity");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Class Id Missing. Cannot fetch Student Performance in Daily Class Activity"),
              ExecutionStatus.FAILED);
    }

    this.userId = this.context.request().getString(REQUEST_USERID);
    List<String> userIds = new ArrayList<>();
    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is not in the request to fetch Student Performance in Course. Asseume user is a teacher");
      LazyList<AJEntityBaseReports> userIdOfClass =
              AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_DAILY_CLASS_ACTIVITY, this.classId, this.collectionType);
      userIdOfClass.forEach(users -> userIds.add(users.getString(AJEntityBaseReports.GOORUUID)));

    } else {
      userIds.add(this.userId);
    }

    for (String userId : userIds) {
      JsonArray assessmentArray = new JsonArray();
      JsonObject dateActivity = new JsonObject();
      
      if (this.collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
    	LOGGER.debug("Fetching Performance for Assessments in Class");
        List<Map> assessmentPerf = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS, classId,
                listToPostgresArrayString(collIds), userId, this.collectionType, AJEntityBaseReports.ATTR_CP_EVENTNAME, startDate, endDate);
        if (!assessmentPerf.isEmpty()) {
          assessmentPerf.forEach(m -> {
            JsonObject assessmentKpi = new JsonObject();
            assessmentKpi.put(AJEntityBaseReports.DATE, m.get(AJEntityBaseReports.ACTIVITY_DATE).toString());
            assessmentKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
            assessmentKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(Double.valueOf(m.get(AJEntityBaseReports.ATTR_SCORE).toString())));
            assessmentKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
            assessmentKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, Integer.parseInt(m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()));
            assessmentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            assessmentArray.add(assessmentKpi);
          });

        } else {
          LOGGER.debug("No data available for ANY of the Assessments passed on to this endpoint");
        }
      } else {
        LOGGER.debug("Fetching Performance for Collections in Class");
        List<Map> collectionPerf = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_CLASS_COLLECTIONS, classId,
                listToPostgresArrayString(collIds), userId, this.collectionType, startDate, endDate);
        if (!collectionPerf.isEmpty()) {
          collectionPerf.forEach(m -> {
            JsonObject collectionKpi = new JsonObject();
            collectionKpi.put(AJEntityBaseReports.DATE, m.get(AJEntityBaseReports.ACTIVITY_DATE).toString());
            collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
            collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
            collectionKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, Integer.parseInt(m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()));
            collectionKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            List<Map> collectionQuestionCount = null;
            collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_CLASS_COLLECTION_QUESTION_COUNT, classId,
                    m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString(), this.userId);

            collectionQuestionCount.forEach(qc -> {
              this.questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
            });
            double scoreInPercent = 0;
            if (this.questionCount > 0) {
              Object collectionScore = null;
              collectionScore = Base.firstCell(AJEntityBaseReports.GET_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE, classId,
                      m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString(), this.userId);
              if (collectionScore != null) {
                scoreInPercent = (((double) Integer.valueOf(collectionScore.toString()) / this.questionCount) * 100);
              }
            }
            collectionKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(scoreInPercent));
            assessmentArray.add(collectionKpi);
          });

        } else {
          LOGGER.debug("No data available for ANY of the Collections passed on to this endpoint");
        }

      }
      dateActivity.put(JsonConstants.ACTIVITY, assessmentArray);
      dateActivity.put(JsonConstants.USERID, userId);
      userUsageArray.add(dateActivity);
    }

    resultBody.put(JsonConstants.USAGE_DATA, userUsageArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  private String listToPostgresArrayString(List<String> input) {
    int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
                                                // 36
                                                // chars
    Iterator<String> it = input.iterator();
    if (!it.hasNext()) {
      return "{}";
    }

    StringBuilder sb = new StringBuilder(approxSize);
    sb.append('{');
    for (;;) {
      String s = it.next();
      sb.append('"').append(s).append('"');
      if (!it.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',');
    }

  }
}
