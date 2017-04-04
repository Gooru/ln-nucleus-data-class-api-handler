package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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
    private String collectionType;
    private String userId;

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
      //FIXME: to be reverted
      /*if (context.getUserIdFromRequest() == null
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> creator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
        if (creator.isEmpty()) {
          List<Map> collaborator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_COLLABORATOR, this.context.classId(), this.context.userIdFromSession());
          if (collaborator.isEmpty()) {
            LOGGER.debug("validateRequest() FAILED");
            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
          }
        }
      }*/
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject resultBody = new JsonObject();
    JsonArray resultarray = new JsonArray();

    // FIXME: Collection Type Accepted "assessment" only.
    this.collectionType = "assessment";
    if (StringUtil.isNullOrEmpty(collectionType)
            || (StringUtil.isNullOrEmpty(collectionType) && this.collectionType.equalsIgnoreCase(AJEntityBaseReports.ATTR_ASSESSMENT))) {
      LOGGER.warn("CollectionType is mandatory to fetch Student Performance in assessment");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse(
                      "CollectionType is missing or make sure collectionType is assessment . Cannot fetch Student Performance in Assessment"),
              ExecutionStatus.FAILED);
    }

    this.userId = this.context.request().getString(REQUEST_USERID);

    List<String> userIds = new ArrayList<>();

    // FIXME : userId can be added as GROUPBY in performance query. Not
    // necessary to get distinct users.
    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
      LazyList<AJEntityBaseReports> userIdforlesson =
              AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_COLLECTION_ID_FILTERBY_COLLTYPE, context.classId(),
                      context.courseId(), context.unitId(), context.lessonId(), context.collectionId(), this.collectionType);
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));

    } else {
      userIds.add(this.userId);
    }

    LOGGER.debug("UID is " + this.userId);

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      List<Map> studentLatestAttempt = null;
      studentLatestAttempt = Base.findAll(AJEntityBaseReports.GET_LATEST_COMPLETED_SESSION_ID, context.classId(), context.courseId(),
              context.unitId(), context.lessonId(), context.collectionId(), userID);

      if (!studentLatestAttempt.isEmpty()) {
        studentLatestAttempt.forEach(attempts -> {
          List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,
                  attempts.get(AJEntityBaseReports.SESSION_ID).toString(), AJEntityBaseReports.ATTR_CRP_EVENTNAME);
          JsonArray questionsArray = new JsonArray();
          if (!assessmentQuestionsKPI.isEmpty()) {
            assessmentQuestionsKPI.stream().forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), questions);
              // FIXME :: This is to be revisited. We should alter the schema
              // column type from TEXT to JSONB. After this change we can remove
              // this logic
              qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
              // FIXME :: it can be removed once we fix writer code.
              qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
              qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())));
              questionsArray.add(qnData);
            });
          }
          contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userID);
        });
      } else {
        // Return an empty resultBody instead of an Error
        LOGGER.debug("No data returned for Student Perf in Assessment");
      }
      resultarray.add(contentBody);
    }
    resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

  }   

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
}
