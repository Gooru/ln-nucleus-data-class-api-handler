package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 * 
 */
public class DCAStudentAnsForRubricQuesHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudentAnsForRubricQuesHandler.class);
  private final ProcessorContext context;

  private String collectionId;
  private String classId;
  private Date activityDate;

  public DCAStudentAnsForRubricQuesHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Answer for Question");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid data provided to fetch Student Answer for Question"),
          ExecutionStatus.FAILED);
    }

    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassID is mandatory to fetch student's answer to grade");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch student's answer"), ExecutionStatus.FAILED);

    }

    this.collectionId = this.context.request().getString(MessageConstants.COLLECTION_ID);
    if (StringUtil.isNullOrEmpty(collectionId)) {
      LOGGER.warn("CollectionID is mandatory to fetch student's answer to grade");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Collection Id Missing. Cannot fetch student's answer"), ExecutionStatus.FAILED);

    }

    String date = this.context.request().getString(MessageConstants.ACTIVITY_DATE);
    if (StringUtil.isNullOrEmpty(date)) {
      LOGGER.warn("Activity Date is mandatory to fetch student list");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Activity Date is Missing. Cannot fetch student list"), ExecutionStatus.FAILED);
    } else {
      this.activityDate = Date.valueOf(date);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
        this.context.request().getString(MessageConstants.CLASS_ID),
        this.context.userIdFromSession());
    if (owner.isEmpty()) {
      LOGGER.debug("validateRequest() FAILED");
      return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
          "User is not authorized for Rubric Grading"), ExecutionStatus.FAILED);
    }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();

    // for this User, for this question, get latest completed session's answer
    Object latestCompletedSessionId =
        Base.firstCell(AJEntityDailyClassActivity.GET_LATEST_COMPLETED_SESSION_ID, classId,
            this.collectionId, context.studentId(), activityDate);
    if (latestCompletedSessionId != null) {

      AJEntityDailyClassActivity ansModel = AJEntityDailyClassActivity.findFirst(
          "class_id = ? AND collection_id = ? AND resource_id = ? AND actor_id = ? AND date_in_time_zone = ? AND "
              + "session_id = ? AND event_name = 'collection.resource.play' AND event_type = 'stop' AND "
              + "resource_type = 'question' AND is_graded = 'false' AND resource_attempt_status = 'attempted' AND "
              + "grading_type = 'teacher' AND question_type = 'OE' order by updated_at desc",
          classId, this.collectionId, context.questionId(), context.studentId(), activityDate,
          latestCompletedSessionId.toString());

      if (ansModel != null) {
        result.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, this.collectionId);
        result.put(AJEntityDailyClassActivity.ATTR_QUESTION_ID, context.questionId());
        result.put(AJEntityDailyClassActivity.ATTR_QUESTION_TEXT, "NA");
        result.put(AJEntityDailyClassActivity.ATTR_ANSWER_TEXT,
            ansModel.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null
                ? new JsonArray(ansModel.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString())
                : null);
        result.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
            Long.parseLong(ansModel.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
        result.put(AJEntityDailyClassActivity.SUBMITTED_AT,
            (ansModel.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString()));
        result.put(AJEntityDailyClassActivity.ATTR_SESSION_ID,
            (ansModel.get(AJEntityDailyClassActivity.SESSION_ID).toString()));
      } else {
        LOGGER.info("Answers cannot be obtained");
      }
    }
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
