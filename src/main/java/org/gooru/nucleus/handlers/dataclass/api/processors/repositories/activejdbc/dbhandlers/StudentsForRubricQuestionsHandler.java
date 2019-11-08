package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
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

public class StudentsForRubricQuestionsHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudentsForRubricQuestionsHandler.class);
  private final ProcessorContext context;
  private String courseId;
  private String classId;
  private String collectionId;

  public StudentsForRubricQuestionsHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Ids for Question to Grade");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid request received to fetch Student Ids for Question to Grade"),
          ExecutionStatus.FAILED);
    }

    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassID is mandatory to fetch student list for this question");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch student list"), ExecutionStatus.FAILED);

    }

    this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);
    if (StringUtil.isNullOrEmpty(courseId)) {
      LOGGER.warn("CourseID is mandatory to fetch student list for this question");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Course Id Missing. Cannot fetch student list"), ExecutionStatus.FAILED);

    }

    this.collectionId = this.context.request().getString(MessageConstants.COLLECTION_ID);
    if (StringUtil.isNullOrEmpty(collectionId)) {
      LOGGER.warn("CollectionID is mandatory to fetch student list");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Collection Id Missing. Cannot fetch student list"), ExecutionStatus.FAILED);

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

    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultarray = new JsonArray();

    LazyList<AJEntityBaseReports> userIdsPendingGradingforSpecifiedQ =
        AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_STUDENTS_FOR_THIS_RESOURCE,
            classId, courseId, collectionId, context.questionId());

    if (!userIdsPendingGradingforSpecifiedQ.isEmpty()) {
      for (AJEntityBaseReports userIdPendingGradingforSpecifiedQ : userIdsPendingGradingforSpecifiedQ) {
        String gradePendingSessionId =
            userIdPendingGradingforSpecifiedQ.get(AJEntityBaseReports.SESSION_ID).toString();
        String studentId =
            userIdPendingGradingforSpecifiedQ.get(AJEntityBaseReports.GOORUUID).toString();
        LOGGER.debug("UID is " + studentId);

        // for this User, for this question, include student to response from latest completed session
        Object latestCompletedSession = Base.firstCell(AJEntityBaseReports.GET_LATEST_COMPLETED_SESSION_ID,
            classId, this.courseId, this.collectionId, context.questionId(), context.studentId());

        if (latestCompletedSession != null) {
          String latestCompletedSessionId = latestCompletedSession.toString();
          if (latestCompletedSessionId.equalsIgnoreCase(gradePendingSessionId)) {
            resultarray.add(studentId);
          }
        }
      }
    } else {
      LOGGER.info("Student list for this Rubric Question cannot be obtained");
    }

    result.put(JsonConstants.STUDENTS, resultarray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
