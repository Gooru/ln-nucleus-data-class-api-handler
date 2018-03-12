package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessment"),
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
    JsonArray resultarray = new JsonArray();

    // FIXME: Collection Type Accepted "assessment" only.
        String collectionType = "assessment";

        String userId = this.context.request().getString(REQUEST_USERID);

    List<String> userIds = new ArrayList<>();

    // FIXME : userId can be added as GROUPBY in performance query. Not
    // necessary to get distinct users.
    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
      LazyList<AJEntityBaseReports> userIdforlesson =
              AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_COLLECTION_ID_FILTERBY_COLLTYPE, context.classId(),
                      context.courseId(), context.unitId(), context.lessonId(), context.collectionId(), collectionType);
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));

    } else {
      userIds.add(userId);
    }

    LOGGER.debug("UID is " + userId);

    for (String userID : userIds) {
      List<Map> studentLatestAttempt;
      studentLatestAttempt = Base.findAll(AJEntityBaseReports.GET_LATEST_COMPLETED_SESSION_ID, context.classId(), context.courseId(),
              context.unitId(), context.lessonId(), context.collectionId(), userID);

      if (!studentLatestAttempt.isEmpty()) {
        JsonObject contentBody = new JsonObject();
        studentLatestAttempt.forEach(attempts -> {
          List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,context.collectionId(),
                  attempts.get(AJEntityBaseReports.SESSION_ID).toString(), AJEntityBaseReports.ATTR_CRP_EVENTNAME);
          LOGGER.debug("latestSessionId : " + attempts.get(AJEntityBaseReports.SESSION_ID));
          JsonArray questionsArray = new JsonArray();
          if (!assessmentQuestionsKPI.isEmpty()) {
            assessmentQuestionsKPI.forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), questions);
              String sessionId = attempts.get(AJEntityBaseReports.SESSION_ID).toString(); 
              qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
              qnData.put(JsonConstants.ANSWER_OBJECT, questions.get(AJEntityBaseReports.ANSWER_OBECT) != null
            		  ? new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()) : null);
              //Rubrics - Score should be NULL only incase of OE questions
              qnData.put(JsonConstants.SCORE, questions.get(AJEntityBaseReports.SCORE) != null ?
            		  Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())) : "NA");
              Object reactionObj = Base.firstCell(AJEntityBaseReports.SELECT_ASSESSMENT_RESOURCE_REACTION, context.collectionId(),
            		  sessionId, questions.get(AJEntityBaseReports.RESOURCE_ID).toString());
              qnData.put(JsonConstants.REACTION, reactionObj != null ? ((Number)reactionObj).intValue() : 0);

              questionsArray.add(qnData);
            });
          }
          contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userID);
        });
        resultarray.add(contentBody);
      } else {
        // Return an empty resultBody instead of an Error
        LOGGER.debug("No data returned for Student Perf in Assessment");
      }
    }
    resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

  }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }
}
