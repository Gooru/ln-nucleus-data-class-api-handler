package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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

public class StudDCACollectionSummaryHandler implements DBHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudDCACollectionSummaryHandler.class);
  private final ProcessorContext context;
  private static final String DATE = "date";
  private String userId;
  private int questionCount = 0;
  private long lastAccessedTime;
  private double maxScore;
  private String sessionId;

  public StudDCACollectionSummaryHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received to fetch Student Performance in Collections");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid data provided to fetch Student Performance in Collections"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          context.request().getString(MessageConstants.CLASS_ID), this.context.userIdFromSession());
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
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject resultBody = new JsonObject();
    JsonObject assessmentDataKPI = new JsonObject();

    this.userId = context.getUserIdFromRequest();
    String classId = context.request().getString(MessageConstants.CLASS_ID);
    // For DCA activities, the summary report should be fetched based only on
    // classId and collectionId. (CourseId, UnitId and lessonId are not expected)
    String todayDate = this.context.request().getString(DATE);
    String collectionId = context.collectionId();
    JsonArray contentArray = new JsonArray();

    // For DCA activities, the summary report should be fetched based only on classId and
    // collectionId. (CourseId, UnitId and lessonId are not expected)
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory to fetch Student Performance in a DCA Collection");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "ClassId Missing. Cannot fetch Collection Summary in DCA"), ExecutionStatus.FAILED);

    }

    if (StringUtil.isNullOrEmpty(todayDate)) {
      LOGGER.warn("Date is mandatory to fetch Student Performance in a DCA Collection");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Date Missing. Cannot fetch Collection Summary in DCA"), ExecutionStatus.FAILED);

    }

    Date date = Date.valueOf(todayDate);

    if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(collectionId)) {
      List<Map> collectionMaximumScore =
          Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_MAX_SCORE, classId,
              collectionId, this.userId, date);
      collectionMaximumScore.forEach(ms -> {
        if (ms.get(AJEntityDailyClassActivity.MAX_SCORE) != null) {
          this.maxScore = Double.valueOf(ms.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
        } else {
          this.maxScore = 0;
        }
      });
    } else {
      this.maxScore = 0;
    }

    List<Map> lastAccessedTime =
        Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_LAST_ACCESSED_TIME, classId,
            collectionId, this.userId, date);

    if (!lastAccessedTime.isEmpty()) {
      lastAccessedTime.forEach(l -> {
        this.lastAccessedTime =
            l.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP) != null ? Timestamp
                .valueOf(l.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString()).getTime()
                : null;
        this.sessionId = l.get(AJEntityDailyClassActivity.SESSION_ID) != null
            ? l.get(AJEntityDailyClassActivity.SESSION_ID).toString()
            : "NA";
      });
    }

    List<Map> collectionData = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_DATA,
        classId, collectionId, this.userId, date);

    if (!collectionData.isEmpty()) {
      LOGGER.debug("Collection Attributes obtained");
      collectionData.forEach(m -> {
        JsonObject assessmentData =
            ValueMapper.map(ResponseAttributeIdentifier.getSessionDCACollectionAttributesMap(), m);
        assessmentData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
        assessmentData.put(EventConstants.SESSION_ID, this.sessionId);
        // Update this to be COLLECTION_TYPE in response
        assessmentData.put(EventConstants.COLLECTION_TYPE,
            m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
        assessmentData.put(JsonConstants.SCORE,
            m.get(AJEntityDailyClassActivity.SCORE) != null
                ? Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString()))
                : null);

        double scoreInPercent;
        int reaction = 0;
        Object collectionScore =
            Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_SCORE, classId,
                collectionId, this.userId, date);

        if (collectionScore != null && (this.maxScore > 0)) {
          scoreInPercent = ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
          assessmentData.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent));
        } else {
          assessmentData.putNull(AJEntityDailyClassActivity.SCORE);
        }

        Object collectionReaction;
        collectionReaction =
            Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_REACTION, classId,
                collectionId, this.userId, date);

        if (collectionReaction != null) {
          reaction = Integer.valueOf(collectionReaction.toString());
        }
        assessmentData.put(AJEntityDailyClassActivity.ATTR_REACTION, (reaction));
        assessmentDataKPI.put(JsonConstants.COLLECTION, assessmentData);
      });

      LOGGER.debug("Collection resource Attributes started");
      List<Map> assessmentQuestionsKPI;
      assessmentQuestionsKPI =
          Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_DATA, classId,
              collectionId, this.userId, date);

      JsonArray questionsArray = new JsonArray();
      if (!assessmentQuestionsKPI.isEmpty()) {
        assessmentQuestionsKPI.forEach(questions -> {
          JsonObject qnData = ValueMapper.map(
              ResponseAttributeIdentifier.getSessionDCACollectionResourceAttributesMap(),
              questions);
          if (questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null) {
            qnData.put(JsonConstants.ANSWER_OBJECT,
                new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()));
          }
          // Default answerStatus will be skipped
          if (qnData.getString(EventConstants.RESOURCE_TYPE)
              .equalsIgnoreCase(EventConstants.QUESTION)) {
            qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
          }

          List<Map> questionScore =
              Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_QUESTION_AGG_SCORE, classId,
                  collectionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId,
                  date);

          if (questionScore != null && !questionScore.isEmpty()) {
            questionScore.forEach(qs -> {
              qnData.put(JsonConstants.ANSWER_OBJECT,
                  qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null
                      ? new JsonArray(qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString())
                      : null);
              // Rubrics - Score may be NULL only incase of OE questions
              qnData.put(JsonConstants.SCORE,
                  qs.get(AJEntityDailyClassActivity.SCORE) != null
                      ? Math.round(
                          Double.valueOf(qs.get(AJEntityDailyClassActivity.SCORE).toString()) * 100)
                      : "NA");
              qnData.put(EventConstants.ANSWERSTATUS,
                  qs.get(AJEntityDailyClassActivity.ATTR_ATTEMPT_STATUS).toString());
            });
          }

          // Get grading status for Questions
          if (qnData.getString(EventConstants.RESOURCE_TYPE)
              .equalsIgnoreCase(EventConstants.QUESTION)
              && qnData.getString(EventConstants.QUESTION_TYPE)
                  .equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)) {
            Object isGradedObj = Base.firstCell(
                AJEntityDailyClassActivity.GET_COLL_OE_QUE_GRADE_STATUS, classId, collectionId,
                questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId, date);
            if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t")
                || isGradedObj.toString().equalsIgnoreCase("true"))) {
              qnData.put(JsonConstants.IS_GRADED, true);
            } else {
              qnData.put(JsonConstants.IS_GRADED, false);
            }
          } else {
            qnData.put(JsonConstants.IS_GRADED, true);
          }

          List<Map> resourceReaction;
          resourceReaction =
              Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_REACTION,
                  classId, collectionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID),
                  this.userId, date);

          if (!resourceReaction.isEmpty()) {
            resourceReaction.forEach(rs -> qnData.put(JsonConstants.REACTION,
                Integer.valueOf(rs.get(AJEntityDailyClassActivity.REACTION).toString())));
          }
          qnData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
          qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
          questionsArray.add(qnData);
        });
      }
      assessmentDataKPI.put(JsonConstants.RESOURCES, questionsArray);
      LOGGER.debug("Collection Attributes obtained");
      contentArray.add(assessmentDataKPI);
    } else {
      LOGGER.info("Collection Attributes cannot be obtained");
    }
    resultBody.put(JsonConstants.CONTENT, contentArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }


}
