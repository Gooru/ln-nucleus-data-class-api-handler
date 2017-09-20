package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;

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

public class QuestionsToGradeHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(QuestionsToGradeHandler.class);
  private final ProcessorContext context;

    private String classId;
  private String courseId;
  private String userId;


  public QuestionsToGradeHandler(ProcessorContext context) {
      this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
      if (context.request() == null || context.request().isEmpty()) {
          LOGGER.warn("Invalid request recieved to fetch Questions to grade");
          return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Questions to grade"),
              ExecutionStatus.FAILED);
      }

      LOGGER.debug("checkSanity() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {

//        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
//        if (owner.isEmpty()) {
//          LOGGER.debug("validateRequest() FAILED");
//          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not authorized for Rubric Grading"), ExecutionStatus.FAILED);
//        }

        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
public ExecutionResult<MessageResponse> executeRequest() {
  JsonObject result = new JsonObject();
  JsonArray resultArray = new JsonArray();

      AJEntityBaseReports baseReport = new AJEntityBaseReports();

  this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
  if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassID is mandatory to fetch Questions pending grading");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Class Id Missing. Cannot fetch Questions pending grading"),
              ExecutionStatus.FAILED);

    }

  this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);
  if (StringUtil.isNullOrEmpty(courseId)) {
      LOGGER.warn("CourseID is mandatory to fetch Questions pending grading");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Course Id Missing. Cannot fetch Questions pending grading"),
              ExecutionStatus.FAILED);

    }

	List<Map> queMap = Base.findAll(AJEntityBaseReports.GET_QUESTIONS_TO_GRADE, this.classId, this.courseId);

	if (!queMap.isEmpty()){
	  queMap.forEach(m -> {
    JsonObject que = new JsonObject();
    que.put(AJEntityBaseReports.ATTR_UNIT_ID, m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null);
    que.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null);
    que.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
    String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
    que.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId );
    String resourceId = m.get(AJEntityBaseReports.RESOURCE_ID).toString();
    que.put(AJEntityBaseReports.ATTR_RESOURCE_ID, resourceId);
    int studentCount = 0;
    Object sc;
    sc = Base.firstCell(AJEntityBaseReports.GET_STUDENT_COUNT_FOR_QUESTIONS, this.classId, this.courseId,
            collectionId, resourceId);
    if (sc != null) {
      studentCount = Integer.valueOf(sc.toString());
    }
    que.put(JsonConstants.STUDENT_COUNT, studentCount);
    resultArray.add(que);

  });

	} else {
      LOGGER.info("Questions pending grading cannot be obtained");
  }

  result.put(JsonConstants.GRADE_ITEMS, resultArray)
  .put(AJEntityBaseReports.ATTR_CLASS_ID, this.classId)
  .put(AJEntityBaseReports.ATTR_COURSE_ID, this.courseId);

  //resultBody.put("Rubrics - QuestionsToGrade" , "WORK IN PROGRESS");

  return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

}

  @Override
  public boolean handlerReadOnly() {
      return true;
  }

}
