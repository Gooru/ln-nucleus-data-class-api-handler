package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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
//    if (context.getUserIdFromRequest() == null
//            || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
//      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
//      if (owner.isEmpty()) {
//        LOGGER.debug("validateRequest() FAILED");
//        return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
//      }
//    }
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
      LazyList<AJEntityDailyClassActivity> userIdOfClass =
    		  AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_DAILY_CLASS_ACTIVITY, this.classId, this.collectionType);
      userIdOfClass.forEach(users -> userIds.add(users.getString(AJEntityDailyClassActivity.GOORUUID)));

    } else {
      userIds.add(this.userId);
    }

    for (String userId : userIds) {
      JsonArray assessmentArray = new JsonArray();
      JsonObject dateActivity = new JsonObject();
      
      if (this.collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
    	LOGGER.debug("Fetching Performance for Assessments in Class");
        List<Map> assessmentPerf = Base.findAll(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS, classId,
                listToPostgresArrayString(collIds), userId, this.collectionType, AJEntityDailyClassActivity.ATTR_CP_EVENTNAME, startDate, endDate);
        if (!assessmentPerf.isEmpty()) {
          assessmentPerf.forEach(m -> {
            JsonObject assessmentKpi = new JsonObject();
            assessmentKpi.put(AJEntityDailyClassActivity.DATE, m.get(AJEntityDailyClassActivity.ACTIVITY_DATE).toString());
            assessmentKpi.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString());
            assessmentKpi.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
            assessmentKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID, m.get(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID).toString());
            assessmentKpi.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityDailyClassActivity.ATTR_TIME_SPENT).toString()));
            assessmentKpi.put(AJEntityDailyClassActivity.ATTR_ATTEMPTS, Integer.parseInt(m.get(AJEntityDailyClassActivity.ATTR_ATTEMPTS).toString()));
            assessmentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            assessmentArray.add(assessmentKpi);
          });

        } else {
          LOGGER.debug("No data available for ANY of the Assessments passed on to this endpoint");
        }
      } else {
        LOGGER.debug("Fetching Performance for Collections in Class");
        List<Map> collectionPerf = Base.findAll(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_COLLECTIONS, classId,
                listToPostgresArrayString(collIds), userId, this.collectionType, startDate, endDate);
        if (!collectionPerf.isEmpty()) {
          collectionPerf.forEach(m -> {
            JsonObject collectionKpi = new JsonObject();
            collectionKpi.put(AJEntityDailyClassActivity.DATE, m.get(AJEntityDailyClassActivity.ACTIVITY_DATE).toString());
            collectionKpi.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString());
            collectionKpi.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityDailyClassActivity.ATTR_TIME_SPENT).toString()));
            collectionKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID, AJEntityDailyClassActivity.NA);
            collectionKpi.put(AJEntityDailyClassActivity.ATTR_ATTEMPTS, Integer.parseInt(m.get(AJEntityDailyClassActivity.ATTR_ATTEMPTS).toString()));
            collectionKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            List<Map> collectionQuestionCount = null;
            collectionQuestionCount = Base.findAll(AJEntityDailyClassActivity.SELECT_CLASS_COLLECTION_QUESTION_COUNT, classId,
                    m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString(), this.userId);

            //If questions are not present then Question Count is always zero, however this additional check needs to be added
            //since during migration of data from 3.0 chances are that QC may be null instead of zero
            collectionQuestionCount.forEach(qc -> {
            	if (qc.get(AJEntityBaseReports.QUESTION_COUNT) != null) {
            		this.questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
            	} else {
            		this.questionCount = 0;
            	}              
            });
//            collectionQuestionCount.forEach(qc -> {
//              this.questionCount = Integer.valueOf(qc.get(AJEntityDailyClassActivity.QUESTION_COUNT).toString());
//            });
            double scoreInPercent = 0;
            if (this.questionCount > 0) {
              Object collectionScore = null;
              collectionScore = Base.firstCell(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE, classId,
                      m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString(), this.userId, 
                      Date.valueOf(m.get(AJEntityDailyClassActivity.ACTIVITY_DATE).toString()));
              if (collectionScore != null) {                
                scoreInPercent = (((Double.valueOf(collectionScore.toString())) / this.questionCount) * 100);
              }
              collectionKpi.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(scoreInPercent));
            } else {
            	//If Collections have No Questions then score should be NULL
            	collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
            }            
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
    return true;
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
