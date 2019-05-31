package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOfflineActivitySelfGrade;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOfflineActivitySubmissions;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class DCAStudentSubmissionForOAHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DCAStudentSubmissionForOAHandler.class);
  private final ProcessorContext context;

  private String collectionId;
  private String classId;
  private String studentId;

  public DCAStudentSubmissionForOAHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Submissions for OA");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Student Submissions for OA"), ExecutionStatus.FAILED);
    }

    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
    this.collectionId = this.context.request().getString(MessageConstants.COLLECTION_ID);
    this.studentId = this.context.request().getString(MessageConstants.STUDENT_ID);

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
      return new ExecutionResult<>(
          MessageResponseFactory.createForbiddenResponse("User is not authorized for OA Grading"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    List<Model> ansModel = AJEntityOfflineActivitySelfGrade.where(
        AJEntityOfflineActivitySelfGrade.FETCH_OA_SELF_GRADES, this.classId, this.collectionId,
        this.studentId);
    if (ansModel != null && ansModel.size() > 0) {
      buildOaRubricData(result, ansModel.get(0));
      if (!result.isEmpty()) {
        fetchTasks(result);
      }
    } else {
      LOGGER.info("Student Submissions cannot be obtained");
    }
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  private void buildOaRubricData(JsonObject result, Model m) {
      JsonObject gradeObject = new JsonObject();
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_RUBRIC_ID,
          m.get(AJEntityOfflineActivitySelfGrade.RUBRIC_ID) != null
              ? m.get(AJEntityOfflineActivitySelfGrade.RUBRIC_ID).toString()
              : null);

      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_TIME_SPENT,
          m.get(AJEntityOfflineActivitySelfGrade.TIME_SPENT) != null
              ? Long.valueOf(m.get(AJEntityOfflineActivitySelfGrade.TIME_SPENT).toString())
              : 0);
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_STUDENT_SCORE,
          (m.get(AJEntityOfflineActivitySelfGrade.STUDENT_SCORE).toString()));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_MAX_SCORE,
          (m.get(AJEntityOfflineActivitySelfGrade.MAX_SCORE).toString()));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_OVERALL_COMMENT,
          m.get(AJEntityOfflineActivitySelfGrade.OVERALL_COMMENT).toString());
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_GRADER,
          (m.get(AJEntityOfflineActivitySelfGrade.GRADER).toString()));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_CREATED_AT,
          (m.get(AJEntityOfflineActivitySelfGrade.CREATED_AT).toString()));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_CATEGORY_GRADE,
          m.get(AJEntityOfflineActivitySelfGrade.CATEGORY_GRADE) != null
              ? new JsonArray(m.get(AJEntityOfflineActivitySelfGrade.CATEGORY_GRADE).toString())
              : null);
      JsonObject oaRubrics = new JsonObject();
      oaRubrics.put(AJEntityOfflineActivitySelfGrade.ATTR_STUDENT_GRADES, gradeObject);
      oaRubrics.put(AJEntityOfflineActivitySelfGrade.ATTR_TEACHER_GRADES, gradeObject);
      result.put(AJEntityOfflineActivitySelfGrade.ATTR_OA_RUBRICS, oaRubrics);
  }

  private void fetchTasks(JsonObject result) {
    JsonObject taskMap = null;
    List<Model> submissionsModel = AJEntityOfflineActivitySubmissions.where(
        AJEntityOfflineActivitySubmissions.FETCH_OA_SUBMISSIONS, this.classId, this.collectionId,
        this.studentId);
    if (submissionsModel != null && submissionsModel.size() > 0) {
      taskMap = new JsonObject();
      for (Model m : submissionsModel) {
        JsonObject submissionsObject = new JsonObject();
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_ID,
            m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_ID).toString());
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_SUBTYPE,
            m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_SUBTYPE).toString());
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_TYPE,
            m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_TYPE).toString());
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_CREATED_AT,
            (m.get(AJEntityOfflineActivitySubmissions.CREATED_AT).toString()));

        String taskIdString = m.get(AJEntityOfflineActivitySubmissions.TASK_ID).toString();
        JsonArray submissionsArray = new JsonArray();
        if (taskMap.containsKey(taskIdString)) {
          submissionsArray = taskMap.getJsonArray(taskIdString);
          submissionsArray.add(submissionsObject);
        } else {
          submissionsArray.add(submissionsObject);
        }
        taskMap.put(taskIdString, submissionsArray);
      }

      JsonArray taskArray = null;
      if (taskMap != null && !taskMap.isEmpty()) {
        taskArray = new JsonArray();
        for (Entry<String, Object> task : taskMap) {
          JsonObject taskobj = new JsonObject();
          taskobj.put(AJEntityOfflineActivitySubmissions.ATTR_TASK_ID,
              Integer.valueOf(task.getKey()));
          taskobj.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSIONS, task.getValue());
          taskArray.add(taskobj);
        }
      }
      result.put(AJEntityOfflineActivitySubmissions.ATTR_TASKS, taskArray);

    }
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
