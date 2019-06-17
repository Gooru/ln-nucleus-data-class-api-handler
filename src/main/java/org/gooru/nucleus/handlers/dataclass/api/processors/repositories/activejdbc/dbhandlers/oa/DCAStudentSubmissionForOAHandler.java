package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOfflineActivitySelfGrade;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOfflineActivitySubmissions;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
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

  private Long oaDcaId;
  private String classId;
  private String studentId;

  public DCAStudentSubmissionForOAHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {

    this.studentId = this.context.studId();
    this.oaDcaId = Long.valueOf(this.context.oaId().toString());
    this.classId = this.context.classId();
    
    if (StringUtil.isNullOrEmpty(classId) || StringUtil.isNullOrEmpty(studentId) || 
        oaDcaId == null) {
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
    AJEntityDailyClassActivity dcaModel =
        AJEntityDailyClassActivity.findFirst(AJEntityDailyClassActivity.GET_COMPLETED_OA,
            this.classId, this.oaDcaId, this.studentId);
    String sessionId = null;
    if (dcaModel != null
        && dcaModel.getString(AJEntityDailyClassActivity.SESSION_ID) != null) {
      sessionId = dcaModel.getString(AJEntityDailyClassActivity.SESSION_ID);
    }
    return sessionId;
  }

  private void fetchStudentRubricData(JsonObject oaRubrics) {
    List<Model> ansModel = AJEntityOfflineActivitySelfGrade.where(
        AJEntityOfflineActivitySelfGrade.FETCH_OA_SELF_GRADES, this.classId, this.oaDcaId,
        this.studentId);
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
        AJEntityRubricGrading.where(AJEntityRubricGrading.GET_TEACHER_RUBRIC_GRADE_FOR_DCA_OA,
            this.classId, this.oaDcaId, this.studentId);
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
        AJEntityOfflineActivitySubmissions.FETCH_OA_SUBMISSIONS, this.classId, this.oaDcaId,
        this.studentId);
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
          
          AJEntityOfflineActivitySubmissions latestSubmission = AJEntityOfflineActivitySubmissions.findFirst(
              AJEntityOfflineActivitySubmissions.FETCH_OA_LATEST_SUBMISSIONS, this.classId, this.oaDcaId,
              this.studentId, Integer.valueOf(task.getKey()));
          taskobj.put(AJEntityOfflineActivitySubmissions.ATTR_SUBMISSION_TEXT,
              (latestSubmission != null && latestSubmission.get(AJEntityOfflineActivitySubmissions.SUBMISSION_TEXT) != null) ? 
                  latestSubmission.get(AJEntityOfflineActivitySubmissions.SUBMISSION_TEXT).toString() : null);
          
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
