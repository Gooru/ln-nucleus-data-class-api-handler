package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

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

/**
 * Created by daniel
 */

public class StudentAssessmentPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudentAssessmentPerfHandler.class);
  private static final String REQUEST_USERID = "userUid";
  private final ProcessorContext context;

  public StudentAssessmentPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received to fetch Student Performance in Assessment");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid data provided to fetch Student Performance in Assessment"),
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
    // JsonObject assessmentDataKPI = new JsonObject();

    String userId = this.context.request().getString(REQUEST_USERID);

    List<String> userIds = new ArrayList<>();
    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn(
          "UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
      LazyList<AJEntityBaseReports> userIdforlesson = AJEntityBaseReports.findBySQL(
          AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_ASSESSMENT_ID
              + AJEntityBaseReports.ADD_ASS_TYPE_FILTER_TO_QUERY,
          context.classId(), context.courseId(), context.unitId(), context.lessonId(),
          context.collectionId(), AJEntityBaseReports.ATTR_CP_EVENTNAME,
          AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));

    } else {
      userIds.add(userId);
    }

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      AJEntityBaseReports AssessmentPerfModel = AJEntityBaseReports.findFirst(
          "actor_id = ? AND class_id = ? AND course_id = ? AND unit_id = ? "
              + "AND lesson_id = ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop' ORDER BY updated_at DESC",
          userID, context.classId(), context.courseId(), context.unitId(), context.lessonId(),
          context.collectionId());

      if (AssessmentPerfModel != null) {
        String studentLatestSessionId =
            AssessmentPerfModel.getString(AJEntityBaseReports.SESSION_ID);
        Object assessmentReactionObject =
            Base.firstCell(AJEntityBaseReports.SELECT_ASSESSMENT_REACTION, context.collectionId(),
                studentLatestSessionId);
        LOGGER.debug("cID : {} , SID : {} ", context.collectionId(), studentLatestSessionId);

        LOGGER.debug("Assessment Attributes obtained");
        JsonObject assessmentData = new JsonObject();
        assessmentData.put(JsonConstants.SCORE,
            AssessmentPerfModel.get(AJEntityBaseReports.SCORE) != null
                ? Math.round(
                    Double.valueOf(AssessmentPerfModel.get(AJEntityBaseReports.SCORE).toString()))
                : null);
        assessmentData.put(JsonConstants.REACTION,
            assessmentReactionObject != null ? ((Number) assessmentReactionObject).intValue() : 0);
        assessmentData.put(JsonConstants.TIMESPENT,
            AssessmentPerfModel.get(AJEntityBaseReports.TIME_SPENT) != null
                ? AssessmentPerfModel.getLong(AJEntityBaseReports.TIME_SPENT)
                : 0);
        assessmentData.put(JsonConstants.COLLECTION_TYPE,
            AssessmentPerfModel.get(AJEntityBaseReports.COLLECTION_TYPE) != null
                ? AssessmentPerfModel.getString(AJEntityBaseReports.COLLECTION_TYPE)
                : null);
        contentBody.put(JsonConstants.ASSESSMENT, assessmentData);
        List<Map> assessmentQuestionsKPI = Base.findAll(
            AJEntityBaseReports.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,
            context.collectionId(), studentLatestSessionId, AJEntityBaseReports.ATTR_CRP_EVENTNAME);
        LOGGER.debug("latestSessionId : " + studentLatestSessionId);
        JsonArray questionsArray = new JsonArray();
        if (!assessmentQuestionsKPI.isEmpty()) {
          for (Map questions : assessmentQuestionsKPI) {
            JsonObject qnData = ValueMapper.map(
                ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), questions);
            qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
            qnData.put(JsonConstants.ANSWER_OBJECT,
                questions.get(AJEntityBaseReports.ANSWER_OBECT) != null
                    ? new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString())
                    : null);
            // Rubrics - Score should be NULL only incase of OE questions
            qnData.put(JsonConstants.RAW_SCORE,
                questions.get(AJEntityBaseReports.SCORE) != null
                    ? Math
                        .round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString()))
                    : "NA");
            Object reactionObj = Base.firstCell(
                AJEntityBaseReports.SELECT_ASSESSMENT_RESOURCE_REACTION, context.collectionId(),
                studentLatestSessionId, questions.get(AJEntityBaseReports.RESOURCE_ID).toString());
            qnData.put(JsonConstants.REACTION,
                reactionObj != null ? ((Number) reactionObj).intValue() : 0);
            qnData.put(JsonConstants.MAX_SCORE,
                questions.get(AJEntityBaseReports.MAX_SCORE) != null
                    ? Math.round(
                        Double.valueOf(questions.get(AJEntityBaseReports.MAX_SCORE).toString()))
                    : "NA");
            Double quesScore = questions.get(AJEntityBaseReports.SCORE) != null
                ? Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())
                : null;
            Double maxScore = questions.get(AJEntityBaseReports.MAX_SCORE) != null
                ? Double.valueOf(questions.get(AJEntityBaseReports.MAX_SCORE).toString())
                : 0.0;
            double scoreInPercent;
            if (quesScore != null && (maxScore != null && maxScore > 0)) {
              scoreInPercent = ((quesScore / maxScore) * 100);
              qnData.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
            } else {
              qnData.put(AJEntityBaseReports.SCORE, "NA");
            }
            if (qnData.getString(EventConstants.QUESTION_TYPE)
                .equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)) {
              Object isGradedObj = Base.firstCell(AJEntityBaseReports.GET_OE_QUE_GRADE_STATUS,
                  context.collectionId(), studentLatestSessionId,
                  questions.get(AJEntityBaseReports.RESOURCE_ID).toString());
              if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t")
                  || isGradedObj.toString().equalsIgnoreCase("true"))) {
                qnData.put(JsonConstants.IS_GRADED, true);
              } else {
                qnData.put(JsonConstants.IS_GRADED, false);
              }
            } else {
              qnData.put(JsonConstants.IS_GRADED, true);
            }
            questionsArray.add(qnData);
          }
        }
        contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID,
            userID);
        resultarray.add(contentBody);
      } else {
        // Return an empty resultBody instead of an Error
        LOGGER.debug("No data returned for Student Perf in Assessment");
      }
    }
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
