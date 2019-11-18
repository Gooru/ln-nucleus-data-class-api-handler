package org.gooru.nucleus.handlers.dataclass.api.processors.suggestions;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

/**
 * @author Renuka
 */

public class StudentSuggCollectionSummaryHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudentSuggCollectionSummaryHandler.class);
  private final ProcessorContext context;
  private String userId;
  private String classId;
  private Long pathId;
  private String collectionId;
  private double maxScore = 0;
  private long lastAccessedTime;
  private String sessionId;

  public StudentSuggCollectionSummaryHandler(ProcessorContext context) {
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
    if (context.getUserIdFromRequest() == null
        || (!context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, classId,
            this.context.userIdFromSession());
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
    JsonArray contentArray = new JsonArray();

    LOGGER.debug("User ID is " + this.userId);
    LOGGER.debug("cID : {} , pathId : {} ", collectionId, pathId);
    // Getting Question Count
    List<Map> collectionMaximumScore = Base.findAll(
        AJEntityBaseReports.SELECT_SUGG_COLLECTION_MAX_SCORE_, pathId, collectionId, this.userId);

    collectionMaximumScore.forEach(ms -> {
      if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
        this.maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
      } else {
        this.maxScore = 0;
      }
    });

    List<Map> lastAccessedTime =
        Base.findAll(AJEntityBaseReports.SELECT_CLASS_SUGG_COLLECTION_LAST_ACCESSED_TIME, pathId,
            collectionId, this.userId);

    if (!lastAccessedTime.isEmpty()) {
      lastAccessedTime.forEach(l -> {
        this.lastAccessedTime = l.get(AJEntityBaseReports.UPDATE_TIMESTAMP) != null
            ? Timestamp.valueOf(l.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString()).getTime()
            : null;
        this.sessionId = l.get(AJEntityBaseReports.SESSION_ID) != null
            ? l.get(AJEntityBaseReports.SESSION_ID).toString()
            : "NA";
      });
    }

    List<Map> collectionData =
          Base.findAll(AJEntityBaseReports.SELECT_SUGG_COLLECTION_AGG_DATA_, pathId, collectionId, this.userId);

    if (!collectionData.isEmpty()) {
      LOGGER.debug("Collection Attributes obtained");
      collectionData.forEach(m -> {
        JsonObject assessmentData =
            ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionAttributesMap(), m);
        assessmentData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
        assessmentData.put(EventConstants.SESSION_ID, this.sessionId);
        assessmentData.put(EventConstants.RESOURCE_TYPE,
            m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
        assessmentData.put(JsonConstants.SCORE,
            m.get(AJEntityBaseReports.SCORE) != null
                ? Math.round(Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString()))
                : null);

        // With Rubrics Score can be Null (for FR questions)
        double scoreInPercent;
        int reaction = 0;
        Object collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_SUGG_COLLECTION_AGG_SCORE_,
              pathId, collectionId, this.userId);

        if (AJEntityBaseReports.isValidScoreForCollection(collectionScore, this.maxScore)) {
          scoreInPercent = ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
          assessmentData.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
        } else {
          assessmentData.putNull(AJEntityBaseReports.SCORE);
        }

        Object collectionReaction = Base.firstCell(AJEntityBaseReports.SELECT_SUGG_COLLECTION_AGG_REACTION_,
              pathId, collectionId, this.userId);

        if (collectionReaction != null) {
          reaction = Integer.valueOf(collectionReaction.toString());
        }
        LOGGER.debug("Collection reaction : {} - collectionId : {}", reaction, collectionId);
        assessmentData.put(AJEntityBaseReports.ATTR_REACTION, (reaction));
        assessmentDataKPI.put(JsonConstants.COLLECTION, assessmentData);
      });
      LOGGER.debug("Collection resource Attributes started");
      List<Map> assessmentQuestionsKPI = Base.findAll(
            AJEntityBaseReports.SELECT_SUGG_COLLECTION_RESOURCE_AGG_DATA_, pathId, collectionId, this.userId);
      JsonArray questionsArray = new JsonArray();
      if (!assessmentQuestionsKPI.isEmpty()) {
        assessmentQuestionsKPI.forEach(questions -> {
          JsonObject qnData = ValueMapper.map(
              ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
          // FIXME :: This is to be revisited. We should alter the schema column type from TEXT to
          // JSONB. After this change we can remove this logic
          if (questions.get(AJEntityBaseReports.ANSWER_OBECT) != null) {
            qnData.put(JsonConstants.ANSWER_OBJECT,
                new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
          }
          // Default answerStatus will be skipped
          if (qnData.getString(EventConstants.RESOURCE_TYPE)
              .equalsIgnoreCase(EventConstants.QUESTION)) {
            qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
          }
          // qnData.put(JsonConstants.SCORE,
          // Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())));
          List<Map> questionScore = Base.findAll(AJEntityBaseReports.SELECT_SUGG_COLLECTION_QUESTION_AGG_SCORE_,
                pathId, collectionId, questions.get(AJEntityBaseReports.RESOURCE_ID), this.userId);
          String latestSessionId = null;
          if (!questionScore.isEmpty()) {
            latestSessionId = (questionScore.get(0).get(AJEntityBaseReports.SESSION_ID) != null)
                ? questionScore.get(0).get(AJEntityBaseReports.SESSION_ID).toString()
                : null;
            questionScore.forEach(qs -> {
              qnData.put(JsonConstants.ANSWER_OBJECT,
                  qs.get(AJEntityBaseReports.ANSWER_OBECT) != null
                      ? new JsonArray(qs.get(AJEntityBaseReports.ANSWER_OBECT).toString())
                      : null);
              // Rubrics - Score may be NULL only incase of OE questions
              qnData.put(JsonConstants.SCORE,
                  qs.get(AJEntityBaseReports.SCORE) != null
                      ? Math
                          .round(Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString()) * 100)
                      : "NA");
              qnData.put(EventConstants.ANSWERSTATUS,
                  qs.get(AJEntityBaseReports.ATTR_ATTEMPT_STATUS).toString());
            });
          }
          // Get grading status for Questions
          if (!StringUtil.isNullOrEmpty(classId)) {
            if (latestSessionId != null
                && qnData.getString(EventConstants.RESOURCE_TYPE)
                    .equalsIgnoreCase(EventConstants.QUESTION)
                && qnData.getString(EventConstants.QUESTION_TYPE)
                    .equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)) {
              Object isGradedObj = Base.firstCell(AJEntityBaseReports.GET_OE_QUE_GRADE_STATUS,
                  collectionId, latestSessionId, questions.get(AJEntityBaseReports.RESOURCE_ID));
              if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t")
                  || isGradedObj.toString().equalsIgnoreCase("true"))) {
                qnData.put(JsonConstants.IS_GRADED, true);
              } else {
                qnData.put(JsonConstants.IS_GRADED, false);
              }
            } else {
              qnData.put(JsonConstants.IS_GRADED, true);
            }
          }

          List<Map> resourceReaction =
                Base.findAll(AJEntityBaseReports.SELECT_SUGG_COLLECTION_RESOURCE_AGG_REACTION_,
                    pathId, collectionId, questions.get(AJEntityBaseReports.RESOURCE_ID), this.userId);
          if (!resourceReaction.isEmpty()) {
            resourceReaction.forEach(rs -> {
              qnData.put(JsonConstants.REACTION,
                  Integer.valueOf(rs.get(AJEntityBaseReports.REACTION).toString()));
              LOGGER.debug("Resource reaction: {} - resourceId : {}",
                  rs.get(AJEntityBaseReports.REACTION).toString(),
                  questions.get(AJEntityBaseReports.RESOURCE_ID));
            });
          }
          qnData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
          qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
          questionsArray.add(qnData);
        });
      }
      // JsonArray questionsArray =
      // ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(),
      // assessmentQuestionsKPI);
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

  private void validateContextRequest() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Suggestion Collection Summary");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Student Suggestion Collection Summary"));
    }
  }
  
  private void initializeRequestParams() {
    this.userId = context.getUserIdFromRequest();
    this.classId = context.request().containsKey(EventConstants.CLASS_GOORU_OID) ? context.request().getString(EventConstants.CLASS_GOORU_OID) : null;
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
