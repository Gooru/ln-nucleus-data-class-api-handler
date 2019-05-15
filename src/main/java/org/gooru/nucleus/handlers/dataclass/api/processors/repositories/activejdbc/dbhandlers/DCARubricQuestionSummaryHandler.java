package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
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
public class DCARubricQuestionSummaryHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DCARubricQuestionSummaryHandler.class);
  private final ProcessorContext context;  
  private String studentId;
  private String sessionId;
  private Date activityDate;

  public DCARubricQuestionSummaryHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request recieved to fetch Rubric Question Summary");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Rubric Question Summary"), ExecutionStatus.FAILED);
    }
    this.studentId = this.context.request().getString(MessageConstants.STUDENTID);
    if (StringUtil.isNullOrEmpty(studentId)) {
      LOGGER.warn("StudentID is mandatory to fetch Rubric Question Summary");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Student Id Missing. Cannot fetch Rubric Question Summary"), ExecutionStatus.FAILED);
    }
    this.sessionId = this.context.request().getString(MessageConstants.SESSION_ID);
    if (StringUtil.isNullOrEmpty(sessionId)) {
      LOGGER.warn("SessionID is mandatory to fetch Rubric Question Summary");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Question Id Missing. Cannot fetch Rubric Question Summary"), ExecutionStatus.FAILED);
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
    // if (!context.userIdFromSession()
    // .equals(this.context.request().getString(MessageConstants.STUDENTID))) {
    // List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
    // this.context.classId(), this.context.userIdFromSession());
    // if (owner.isEmpty()) {
    // LOGGER.debug("validateRequest() FAILED");
    // return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
    // "User is not authorized for Rubric Grading"), ExecutionStatus.FAILED);
    // }
    // }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultArray = new JsonArray();

    // Currently its assumed that teacher will have only one attempt at grading a student's
    // question, so there should be
    // only one record per session, per student for this question on a given date.
    List<Map> summaryMap = Base.findAll(AJEntityRubricGrading.GET_RUBRIC_GRADE_FOR_DCA_QUESTION,
        context.classId(), context.collectionId(), context.questionId(),
        this.studentId, sessionId, activityDate);

    if (!summaryMap.isEmpty()) {
      summaryMap.forEach(m -> {
        JsonObject smry = new JsonObject();
        smry.put(AJEntityRubricGrading.ATTR_STUDENT_ID, this.studentId);
        smry.put(AJEntityRubricGrading.ATTR_STUDENT_SCORE,
            m.get(AJEntityRubricGrading.STUDENT_SCORE) != null
                ? Math.round(Double.valueOf(m.get(AJEntityRubricGrading.STUDENT_SCORE).toString()))
                : null);
        smry.put(AJEntityRubricGrading.ATTR_MAX_SCORE,
            m.get(AJEntityRubricGrading.MAX_SCORE) != null
                ? Math.round(Double.valueOf(m.get(AJEntityRubricGrading.MAX_SCORE).toString()))
                : null);
        smry.put(AJEntityRubricGrading.ATTR_OVERALL_COMMENT,
            m.get(AJEntityRubricGrading.OVERALL_COMMENT) != null
                ? (m.get(AJEntityRubricGrading.OVERALL_COMMENT).toString())
                : null);
        smry.put(AJEntityRubricGrading.ATTR_CATEGORY_SCORE,
            m.get(AJEntityRubricGrading.CATEGORY_SCORE) != null
                ? new JsonArray((m.get(AJEntityRubricGrading.CATEGORY_SCORE).toString()))
                : null);

        resultArray.add(smry);
      });

    } else {
      LOGGER.info("Rubric Question Summary cannot be obtained");
    }

    result.put(JsonConstants.QUE_RUBRICS, resultArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }


}
