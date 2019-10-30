package org.gooru.nucleus.handlers.dataclass.api.processors.suggestions;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
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

public class StudDCASuggCollectionSummaryHandler implements DBHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudDCASuggCollectionSummaryHandler.class);
  private final ProcessorContext context;
  private static final String DATE = "date";
  private String userId;
  private String classId;
  private Long pathId;
  private String collectionId;
  private long lastAccessedTime;
  private double maxScore;
  private String sessionId;

  public StudDCASuggCollectionSummaryHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    validateContextRequest();
    initializeRequestParams();
    validateContextRequestFields();

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
    JsonObject activityDataKPI = new JsonObject();

    // For DCA activities, the summary report should be fetched based only on
    // classId, collectionId and pathId. (CourseId, UnitId and lessonId are not expected)
    JsonArray contentArray = new JsonArray();

    List<Map> collectionMaximumScore =
        Base.findAll(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_MAX_SCORE, pathId, classId,
            collectionId, this.userId);
    collectionMaximumScore.forEach(ms -> {
      if (ms.get(AJEntityDailyClassActivity.MAX_SCORE) != null) {
        this.maxScore = Double.valueOf(ms.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
      } else {
        this.maxScore = 0;
      }
    });

    List<Map> lastAccessedTime =
        Base.findAll(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_LAST_ACCESSED_TIME, pathId,
            classId, collectionId, this.userId);

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

    List<Map> collectionData =
        Base.findAll(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_AGG_DATA, pathId, classId,
            collectionId, this.userId);

    if (!collectionData.isEmpty()) {
      LOGGER.debug("Collection Attributes obtained");
      collectionData.forEach(m -> {
        JsonObject activityData =
            ValueMapper.map(ResponseAttributeIdentifier.getSessionDCACollectionAttributesMap(), m);
        activityData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
        activityData.put(EventConstants.SESSION_ID, this.sessionId);
        // Update this to be COLLECTION_TYPE in response
        activityData.put(EventConstants.COLLECTION_TYPE,
            m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
        activityData.put(JsonConstants.SCORE,
            m.get(AJEntityDailyClassActivity.SCORE) != null
                ? Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString()))
                : null);

        double scoreInPercent;
        int reaction = 0;
        Object collectionScore =
            Base.firstCell(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_AGG_SCORE, pathId,
                classId, collectionId, this.userId);

        if (AJEntityDailyClassActivity.isValidScoreForCollection(collectionScore, this.maxScore)) {
          scoreInPercent = ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
          activityData.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent));
        } else {
          activityData.putNull(AJEntityDailyClassActivity.SCORE);
        }

        Object collectionReaction;
        collectionReaction =
            Base.firstCell(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_AGG_REACTION, pathId,
                classId, collectionId, this.userId);

        if (collectionReaction != null) {
          reaction = Integer.valueOf(collectionReaction.toString());
        }
        activityData.put(AJEntityDailyClassActivity.ATTR_REACTION, (reaction));
        activityDataKPI.put(JsonConstants.COLLECTION, activityData);
      });

      LOGGER.debug("Collection resource Attributes started");
      List<Map> activityQuestionsKPI;
      activityQuestionsKPI =
          Base.findAll(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_RESOURCE_AGG_DATA, pathId,
              classId, collectionId, this.userId);

      JsonArray questionsArray = new JsonArray();
      if (!activityQuestionsKPI.isEmpty()) {
        activityQuestionsKPI.forEach(questions -> {
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

          List<Map> questionScore = Base.findAll(
              AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_QUESTION_AGG_SCORE, pathId, classId,
              collectionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId);

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
                AJEntityDailyClassActivity.GET_SUGG_COLL_OE_QUE_GRADE_STATUS, pathId, classId,
                collectionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId);
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
              Base.findAll(AJEntityDailyClassActivity.SELECT_SUGG_COLLECTION_RESOURCE_AGG_REACTION,
                  pathId, classId, collectionId,
                  questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId);

          if (!resourceReaction.isEmpty()) {
            resourceReaction.forEach(rs -> qnData.put(JsonConstants.REACTION,
                Integer.valueOf(rs.get(AJEntityDailyClassActivity.REACTION).toString())));
          }
          qnData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
          qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
          questionsArray.add(qnData);
        });
      }
      activityDataKPI.put(JsonConstants.RESOURCES, questionsArray);
      LOGGER.debug("Collection Attributes obtained");
      contentArray.add(activityDataKPI);
    } else {
      LOGGER.info("Collection Attributes cannot be obtained");
    }
    resultBody.put(JsonConstants.CONTENT, contentArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);

  }


  private void validateContextRequest() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Suggestion Collection Summary");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Student Suggestion Collection Summary"));
    }
  }

  private void initializeRequestParams() {
    this.userId = context.getUserIdFromRequest();
    this.classId = context.request().containsKey(EventConstants.CLASS_ID) ? context.request().getString(EventConstants.CLASS_ID) : null;
    this.collectionId = context.collectionId();
  }

  private void validateContextRequestFields() {
    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("userId is mandatory to fetch Student Suggestion Summary.");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "userId is Missing. Cannot fetch Student Suggestion Performance"));
    }
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory for fetching Student Suggestion Summary");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch Student Suggestion Performance"));
    }
    String pId = this.context.request().getString(JsonConstants.PATH_ID);
    if (StringUtil.isNullOrEmpty(pId)) {
      LOGGER.warn("PathId is mandatory to fetch Student Suggestion Summary");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "PathId is Missing. Cannot fetch Student Suggestion Performance"));
    }
    try {
        Long path = Long.valueOf(pId.toString());
        this.pathId = (path > 0) ? path : null;
    } catch (NumberFormatException nfe) {
      throw new MessageResponseWrapperException(
          MessageResponseFactory.createInvalidRequestResponse(
              "NumberFormatException:Invalid pathIds provided to fetch Student Suggestion in class"));
    }
    if (pathId == null) {
      LOGGER.warn("PathId is mandatory to fetch Student Suggestion Summary");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "PathId is Missing. Cannot fetch Student Suggestion Performance"));
    }
    if (StringUtil.isNullOrEmpty(collectionId)) {
      LOGGER.warn("CollectionId is mandatory to fetch Student Suggestion Summary");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "CollectionId is Missing. Cannot fetch Student Suggestion Summary"));
    }
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }


}
