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

  @SuppressWarnings("unchecked")
  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultarray = new JsonArray();

    // for this question, get all distinct users who needs to be graded
    LazyList<AJEntityBaseReports> userIdsPendingGradingforSpecifiedQ =
        AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_STUDENTS_FOR_THIS_RESOURCE,
            classId, courseId, collectionId, context.questionId());

    // for this collection, get distinct users of completed sessions
    LazyList<AJEntityBaseReports> studentsWithLatestSessions = AJEntityBaseReports.findBySQL(
        AJEntityBaseReports.GET_LATEST_SESSION_AND_STUDENTS_FOR_THIS_COLLECTION, classId, courseId,
        this.collectionId);

    if (!studentsWithLatestSessions.isEmpty()) {
      for (AJEntityBaseReports studentsWithLatestSession : studentsWithLatestSessions) {
        String userOfLatestCompletedSession =
            studentsWithLatestSession.getString(AJEntityBaseReports.GOORUUID);

        if (!userIdsPendingGradingforSpecifiedQ.isEmpty()) {
          List<String> usersWithPendingGrade =
              userIdsPendingGradingforSpecifiedQ.collect(AJEntityBaseReports.GOORUUID);
          if (usersWithPendingGrade.contains(userOfLatestCompletedSession)) {
            LOGGER.debug("UID is " + userOfLatestCompletedSession);
            resultarray.add(userOfLatestCompletedSession);
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
