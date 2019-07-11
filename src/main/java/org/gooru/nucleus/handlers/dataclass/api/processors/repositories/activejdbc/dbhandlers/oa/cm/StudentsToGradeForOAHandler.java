package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa.cm;

import java.util.List;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.validators.ValidationUtils;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class StudentsToGradeForOAHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudentsToGradeForOAHandler.class);
  private final ProcessorContext context;
  private String classId;
  private String courseId;
  private String collectionId;

  public StudentsToGradeForOAHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Ids for OA Grading");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid request received to fetch Student Ids for OA Grading"),
          ExecutionStatus.FAILED);
    } else if (context.request() != null || !context.request().isEmpty()) {
      this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
      this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);
      this.collectionId = this.context.request().getString(MessageConstants.ITEM_ID);
      if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(courseId)
          || !ValidationUtils.isValidUUID(collectionId)) {
        LOGGER.warn("Invalid Json Payload");
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
            ExecutionStatus.FAILED);
      }

    }
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    // List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.classId,
    // this.context.userIdFromSession());
    // if (owner.isEmpty()) {
    // LOGGER.debug("validateRequest() FAILED");
    // return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
    // "User is not authorized for OA Grading"), ExecutionStatus.FAILED);
    // }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultarray = new JsonArray();

    LazyList<AJEntityOACompletionStatus> userIdforOA = AJEntityOACompletionStatus.findBySQL(
        AJEntityOACompletionStatus.GET_DISTINCT_STUDENTS_FOR_THIS_CM_OA, classId, courseId, collectionId);

    if (userIdforOA != null && !userIdforOA.isEmpty()) {
      List<String> userIds = userIdforOA.collect(AJEntityOACompletionStatus.STUDENT_ID);
      resultarray = new JsonArray(userIds);
    } else {
      LOGGER.info("Student list for this Offline Activity grading cannot be obtained");
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
