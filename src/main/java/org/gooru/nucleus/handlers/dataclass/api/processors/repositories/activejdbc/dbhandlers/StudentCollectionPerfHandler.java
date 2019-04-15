package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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

public class StudentCollectionPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudentCollectionPerfHandler.class);
  private static final String REQUEST_USERID = "userUid";
  private final ProcessorContext context;
  private double maxScore = 0.0;
  private long lastAccessedTime;
  private String sessionId;

  public StudentCollectionPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Performance in Collection");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid data provided to fetch Student Performance in Collection"),
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
      LOGGER.info("the class_id is " + context.classId());
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
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject resultBody = new JsonObject();
    JsonArray resultarray = new JsonArray();

    String classId = this.context.classId();
    String courseId = this.context.courseId();
    String unitId = this.context.unitId();
    String lessonId = this.context.lessonId();
    String collectionId = this.context.collectionId();
    String userId = this.context.request().getString(REQUEST_USERID);
    List<String> userIds = new ArrayList<>();

    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn(
          "UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
      LazyList<AJEntityBaseReports> userIdforlesson = AJEntityBaseReports.findBySQL(
          AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_COLLECTION_ID
              + AJEntityBaseReports.ADD_COLL_TYPE_FILTER_TO_QUERY,
          context.classId(), context.courseId(), context.unitId(), context.lessonId(),
          context.collectionId());
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));

    } else {
      userIds.add(userId);
    }

    for (String userID : userIds) {
      // Getting Question Count
      List<Map> collectionMaximumScore;
      if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
          && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
        collectionMaximumScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_MAX_SCORE,
            classId, courseId, unitId, lessonId, collectionId, userID);
      } else {
        collectionMaximumScore =
            Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_MAX_SCORE_, collectionId, userID);
      }

      if (collectionMaximumScore != null && !collectionMaximumScore.isEmpty()) {
        collectionMaximumScore.forEach(ms -> {
          if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
            this.maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
          } else {
            this.maxScore = 0.0;
          }
        });
      } else {
        this.maxScore = 0.0;
      }

      List<Map> lastAccessedTime;
      if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
          && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
        lastAccessedTime = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_LAST_ACCESSED_TIME,
            classId, courseId, unitId, lessonId, collectionId, userID);
      } else {
        lastAccessedTime = Base.findAll(
            AJEntityBaseReports.SELECT_CLASS_COLLECTION_LAST_ACCESSED_TIME, collectionId, userID);
      }

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
      JsonObject contentBody = new JsonObject();
      List<Map> collectionData;
      if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
          && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
        collectionData = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_AGG_DATA, classId,
            courseId, unitId, lessonId, collectionId, userID);
      } else {
        collectionData =
            Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_AGG_DATA_, collectionId, userID);
      }
      if (!collectionData.isEmpty()) {
        LOGGER.debug("Collection Attributes obtained");
        collectionData.forEach(m -> {
          JsonObject collectionDataObj =
              ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionAttributesMap(), m);
          collectionDataObj.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
          collectionDataObj.put(EventConstants.SESSION_ID, this.sessionId);
          collectionDataObj.put(EventConstants.RESOURCE_TYPE,
              m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
          collectionDataObj.put(JsonConstants.SCORE,
              m.get(AJEntityBaseReports.SCORE) != null
                  ? Math.round(Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString()))
                  : null);
          collectionDataObj.put(JsonConstants.MAX_SCORE, this.maxScore);

          // With Rubrics Score can be Null (for FR questions)
          double scoreInPercent;
          int reaction = 0;
          Object collectionScore;
          if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
              && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
            collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE,
                classId, courseId, unitId, lessonId, collectionId, userID);
          } else {
            collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE_,
                collectionId, userID);
          }

          if (collectionScore != null && (this.maxScore > 0)) {
            scoreInPercent = ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
            collectionDataObj.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
          } else {
            collectionDataObj.putNull(AJEntityBaseReports.SCORE);
          }

          Object collectionReaction;
          if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
              && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
            collectionReaction = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_REACTION,
                classId, courseId, unitId, lessonId, collectionId, userID);
          } else {
            collectionReaction = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_REACTION_,
                collectionId, userID);
          }
          if (collectionReaction != null) {
            reaction = Integer.valueOf(collectionReaction.toString());
          }
          LOGGER.debug("Collection reaction : {} - collectionId : {}", reaction, collectionId);
          collectionDataObj.put(AJEntityBaseReports.ATTR_REACTION, (reaction));
          contentBody.put(JsonConstants.COLLECTION, collectionDataObj);
        });

        LOGGER.debug("Collection resource Attributes started");
        List<Map> collectionQuestionsKPI;
        if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
            && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
          collectionQuestionsKPI =
              Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA, classId,
                  courseId, unitId, lessonId, collectionId, userID);
        } else {
          collectionQuestionsKPI = Base.findAll(
              AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA_, collectionId, userID);
        }
        JsonArray questionsArray = new JsonArray();
        if (!collectionQuestionsKPI.isEmpty()) {
          collectionQuestionsKPI.forEach(questions -> {
            JsonObject qnData = ValueMapper.map(
                ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
            if (questions.get(AJEntityBaseReports.ANSWER_OBECT) != null) {
              qnData.put(JsonConstants.ANSWER_OBJECT,
                  new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
            }
            // Default answerStatus will be skipped
            if (qnData.getString(EventConstants.RESOURCE_TYPE)
                .equalsIgnoreCase(EventConstants.QUESTION)) {
              qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
            }
            List<Map> questionScore;
            if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
                && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
              questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE,
                  classId, courseId, unitId, lessonId, collectionId,
                  questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
            } else {
              questionScore =
                  Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE_,
                      collectionId, questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
            }
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
                qnData.put(JsonConstants.RAW_SCORE,
                    qs.get(AJEntityBaseReports.SCORE) != null
                        ? Math.round(Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString()))
                        : "NA");
                qnData.put(EventConstants.ANSWERSTATUS,
                    qs.get(AJEntityBaseReports.ATTR_ATTEMPT_STATUS).toString());
                qnData.put(JsonConstants.MAX_SCORE,
                    qs.get(AJEntityBaseReports.MAX_SCORE) != null
                        ? Double.valueOf(qs.get(AJEntityBaseReports.MAX_SCORE).toString())
                        : "NA");
                Double quesScore = qs.get(AJEntityBaseReports.SCORE) != null
                    ? Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString())
                    : null;
                Double maxScore = qs.get(AJEntityBaseReports.MAX_SCORE) != null
                    ? Double.valueOf(qs.get(AJEntityBaseReports.MAX_SCORE).toString())
                    : 0.0;
                double scoreInPercent;
                if (quesScore != null && (maxScore != null && maxScore > 0)) {
                  scoreInPercent = ((quesScore / maxScore) * 100);
                  qnData.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
                } else {
                  qnData.put(AJEntityBaseReports.SCORE, "NA");
                }
              });
            }
            // Get grading status for Questions
            if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
                && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
              if (latestSessionId != null && qnData.getString(EventConstants.QUESTION_TYPE)
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
                qnData.put(JsonConstants.IS_GRADED, "NA");
              }

            }

            List<Map> resourceReaction;
            if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId)
                && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
              resourceReaction =
                  Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_REACTION, classId,
                      courseId, unitId, lessonId, collectionId,
                      questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
            } else {
              resourceReaction =
                  Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_REACTION_,
                      collectionId, questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
            }
            if (!resourceReaction.isEmpty()) {
              resourceReaction.forEach(rs -> {
                qnData.put(JsonConstants.REACTION,
                    Integer.valueOf(rs.get(AJEntityBaseReports.REACTION).toString()));
                LOGGER.debug("Resource reaction: {} - resourceId : {}",
                    rs.get(AJEntityBaseReports.REACTION).toString(),
                    questions.get(AJEntityBaseReports.RESOURCE_ID));
              });
            }
            qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
            questionsArray.add(qnData);

          });
        }
        contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID,
            userID);
        resultarray.add(contentBody);

      } else {
        LOGGER.debug("No data returned for this Student for this Collection");
      }
    } // loop per user
    resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE)
        .putNull(JsonConstants.PAGINATE);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
