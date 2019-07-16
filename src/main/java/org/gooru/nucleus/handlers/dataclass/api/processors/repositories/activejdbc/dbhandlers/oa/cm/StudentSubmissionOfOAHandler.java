package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa.cm;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOfflineActivitySelfGrade;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOfflineActivitySubmissions;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.validators.ValidationUtils;
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
public class StudentSubmissionOfOAHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudentSubmissionOfOAHandler.class);
  private final ProcessorContext context;

  private String classId;
  private String studentId;
  private String courseId;
  private String unitId;
  private String lessonId;
  private String collectionId;

  public StudentSubmissionOfOAHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {

    this.studentId = this.context.studId();
    this.classId = this.context.classId();
    this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);
    this.unitId = this.context.request().getString(MessageConstants.UNIT_ID);
    this.lessonId = this.context.request().getString(MessageConstants.LESSON_ID);
    this.collectionId = this.context.oaId();

    if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(studentId)
        || !ValidationUtils.isValidUUID(courseId) || !ValidationUtils.isValidUUID(unitId)
        || !ValidationUtils.isValidUUID(lessonId) || !ValidationUtils.isValidUUID(collectionId)) {
      LOGGER.warn("Invalid Json Payload");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {    
      if (context.userIdFromSession().equalsIgnoreCase(this.studentId)) {
        LOGGER.debug("Student - validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
      } else {
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
            this.classId, this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(
              MessageResponseFactory.createForbiddenResponse("User is not authorized for OA Grading"),
              ExecutionStatus.FAILED);
        }
        LOGGER.debug("Teacher - validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);          
      }
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    
    JsonArray taskArray = fetchTasks(result);
    result.put(AJEntityOfflineActivitySubmissions.ATTR_TASKS, taskArray);
    
    JsonObject oaRubrics = new JsonObject();
    fetchStudentRubricData(oaRubrics);
    fetchTeacherRubricData(oaRubrics);
    result.put(AJEntityOfflineActivitySelfGrade.ATTR_OA_RUBRICS, oaRubrics);
    
    result.put(AJEntityRubricGrading.ATTR_SESSION_ID, getSessionId());

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  private String getSessionId() {
    AJEntityBaseReports brModel =
        AJEntityBaseReports.findFirst(AJEntityBaseReports.GET_COMPLETED_OA,
            this.classId, this.studentId, this.courseId, this.unitId, this.lessonId, this.collectionId);
    String sessionId = null;
    if (brModel != null
        && brModel.getString(AJEntityBaseReports.SESSION_ID) != null) {
      sessionId = brModel.getString(AJEntityBaseReports.SESSION_ID);
    }
    return sessionId;
  }

  private void fetchStudentRubricData(JsonObject oaRubrics) {
    List<Model> ansModel = AJEntityOfflineActivitySelfGrade.where(
        AJEntityOfflineActivitySelfGrade.FETCH_CM_OA_SELF_GRADES, this.classId,
        this.studentId, this.courseId, this.unitId, this.lessonId, this.collectionId);
    JsonObject gradeObject = null;
    if (ansModel != null && ansModel.size() > 0) {
      gradeObject = new JsonObject();
      Model m = ansModel.get(0);
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_RUBRIC_ID,
          m.get(AJEntityOfflineActivitySelfGrade.RUBRIC_ID) != null
              ? m.get(AJEntityOfflineActivitySelfGrade.RUBRIC_ID).toString()
              : null);
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_TIME_SPENT,
          m.get(AJEntityOfflineActivitySelfGrade.TIME_SPENT) != null
              ? Long.valueOf(m.get(AJEntityOfflineActivitySelfGrade.TIME_SPENT).toString())
              : 0);
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_STUDENT_SCORE,
          (m.get(AJEntityOfflineActivitySelfGrade.STUDENT_SCORE) != null
              ? Double.valueOf(m.get(AJEntityOfflineActivitySelfGrade.STUDENT_SCORE).toString())
              : null));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_MAX_SCORE,
          (m.get(AJEntityOfflineActivitySelfGrade.MAX_SCORE) != null
              ? Double.valueOf(m.get(AJEntityOfflineActivitySelfGrade.MAX_SCORE).toString())
              : null));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_OVERALL_COMMENT,
          m.get(AJEntityOfflineActivitySelfGrade.OVERALL_COMMENT) != null
              ? m.get(AJEntityOfflineActivitySelfGrade.OVERALL_COMMENT).toString()
              : null);
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_GRADER,
          (m.get(AJEntityOfflineActivitySelfGrade.GRADER) != null
              ? m.get(AJEntityOfflineActivitySelfGrade.GRADER).toString()
              : null));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_SUBMITTED_ON,
          (m.get(AJEntityOfflineActivitySelfGrade.UPDATED_AT).toString()));
      gradeObject.put(AJEntityOfflineActivitySelfGrade.ATTR_CATEGORY_SCORE,
          m.get(AJEntityOfflineActivitySelfGrade.CATEGORY_SCORE) != null
              ? new JsonArray(m.get(AJEntityOfflineActivitySelfGrade.CATEGORY_SCORE).toString())
              : null);
    } else {
      LOGGER.info("Student Grading cannot be obtained");
    }
      oaRubrics.put(AJEntityOfflineActivitySelfGrade.ATTR_STUDENT_GRADES, gradeObject);
  }
  
  private void fetchTeacherRubricData(JsonObject oaRubrics) {
    List<Model> ansModel =
        AJEntityRubricGrading.where(AJEntityRubricGrading.GET_TEACHER_RUBRIC_GRADE_FOR_CM_OA,
            this.classId, this.studentId, this.courseId, this.unitId, this.lessonId, this.collectionId);
    JsonObject gradeObject = null;
    if (ansModel != null && ansModel.size() > 0) {
      gradeObject = new JsonObject();
      Model m = ansModel.get(0);
      gradeObject.put(AJEntityRubricGrading.ATTR_RUBRIC_ID,
          m.get(AJEntityRubricGrading.RUBRIC_ID) != null
              ? m.get(AJEntityRubricGrading.RUBRIC_ID).toString()
              : null);
      gradeObject.put(AJEntityRubricGrading.ATTR_STUDENT_SCORE,
          (m.get(AJEntityRubricGrading.STUDENT_SCORE) != null
              ? Double.valueOf(m.get(AJEntityRubricGrading.STUDENT_SCORE).toString())
              : null));
      gradeObject.put(AJEntityRubricGrading.ATTR_MAX_SCORE,
          (m.get(AJEntityRubricGrading.MAX_SCORE) != null
              ? Double.valueOf(m.get(AJEntityRubricGrading.MAX_SCORE).toString())
              : null));
      gradeObject.put(AJEntityRubricGrading.ATTR_OVERALL_COMMENT,
          m.get(AJEntityRubricGrading.OVERALL_COMMENT) != null
              ? m.get(AJEntityRubricGrading.OVERALL_COMMENT).toString()
              : null);
      gradeObject.put(AJEntityRubricGrading.ATTR_GRADER,
          (m.get(AJEntityRubricGrading.GRADER) != null
              ? m.get(AJEntityRubricGrading.GRADER).toString()
              : null));
      gradeObject.put(AJEntityRubricGrading.ATTR_SUBMITTED_ON,
          (m.get(AJEntityRubricGrading.UPDATE_TIMESTAMP).toString()));
      gradeObject.put(AJEntityRubricGrading.ATTR_CATEGORY_SCORE,
          m.get(AJEntityRubricGrading.CATEGORY_SCORE) != null
              ? new JsonArray(m.get(AJEntityRubricGrading.CATEGORY_SCORE).toString())
              : null);
    } else {
      LOGGER.info("Teacher Grading cannot be obtained");
    }
    oaRubrics.put(AJEntityOfflineActivitySelfGrade.ATTR_TEACHER_GRADES, gradeObject);
  }

  private JsonArray fetchTasks(JsonObject result) {
    JsonArray taskArray = null;
    JsonObject taskMap = null;
    List<Model> taskSubmissionsModel = AJEntityOfflineActivitySubmissions.where(
        AJEntityOfflineActivitySubmissions.FETCH_CM_OA_SUBMISSIONS, this.classId,
        this.studentId, this.courseId, this.unitId, this.lessonId, this.collectionId);
    if (taskSubmissionsModel != null && taskSubmissionsModel.size() > 0) {
      taskMap = new JsonObject();
      for (Model m : taskSubmissionsModel) {
        JsonObject submissionsObject = new JsonObject();
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_INFO,
            m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_INFO) != null ? 
                m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_INFO).toString() : null);
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_SUBTYPE,
            m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_SUBTYPE) != null ?
              m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_SUBTYPE).toString() : null);
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_TYPE,
            m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_TYPE) != null ? 
                m.get(AJEntityOfflineActivitySubmissions.SUBMISSION_TYPE).toString() : null);
        submissionsObject.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMITTED_ON,
            (m.get(AJEntityOfflineActivitySubmissions.UPDATED_AT).toString()));

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
    } else {
      LOGGER.info("No tasks available for this student");
    }
    return taskArray;
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
