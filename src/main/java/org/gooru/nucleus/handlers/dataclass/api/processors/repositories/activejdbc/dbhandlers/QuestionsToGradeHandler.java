package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.request().getString(MessageConstants.CLASS_ID), 
        		this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not authorized for Rubric Grading"), ExecutionStatus.FAILED);
        }

        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
public ExecutionResult<MessageResponse> executeRequest() {
  JsonObject result = new JsonObject();
  JsonArray resultArray = new JsonArray();
  Map<String, Integer> resourceStudentMap = new HashMap<>();
  Map<String, JsonObject> resourceCCULMap = new HashMap<>();

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
			String lessonId = m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null;
			String unitId = m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null;
			String collectionType = m.get(AJEntityBaseReports.COLLECTION_TYPE).toString();
			String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
			String resourceId = m.get(AJEntityBaseReports.RESOURCE_ID).toString();
			String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
			String studentId = m.get(AJEntityBaseReports.GOORUUID).toString();

			JsonObject resCCULObject = new JsonObject();
			resCCULObject.put(AJEntityBaseReports.LESSON_GOORU_OID, lessonId);
			resCCULObject.put(AJEntityBaseReports.UNIT_GOORU_OID, unitId);
			resCCULObject.put(AJEntityBaseReports.COLLECTION_OID, collectionId);
			resCCULObject.put(AJEntityBaseReports.COLLECTION_TYPE, collectionType);

			if(!resourceCCULMap.containsKey(resourceId)) {
				resourceCCULMap.put(resourceId, resCCULObject);
			}    
			//Find the latest Session Id for this User, for this resource in this CCUL
			AJEntityBaseReports sessionModel =  AJEntityBaseReports.findFirst("actor_id = ? AND class_id = ? AND course_id = ? AND unit_id = ? "
					+ "AND lesson_id = ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop' ORDER BY updated_at DESC", 
					studentId, this.classId, this.courseId, unitId, lessonId, collectionId);	
			if (sessionModel != null) {
				String latestSessionId = sessionModel.get(AJEntityBaseReports.SESSION_ID) != null ? sessionModel.get(AJEntityBaseReports.SESSION_ID).toString() : null;
				if (!StringUtil.isNullOrEmpty(latestSessionId) && latestSessionId.equals(sessionId.toString())) {
					if (resourceStudentMap.containsKey(resourceId)) {
						int studCount = resourceStudentMap.get(resourceId) + 1;
						resourceStudentMap.put(resourceId, studCount);
					} else {
						resourceStudentMap.put(resourceId, 1);
					}
				}		
			}
		});
		if (!resourceCCULMap.isEmpty()) {
			resourceCCULMap.forEach((ky, val) -> {				
				if (resourceStudentMap.containsKey(ky)) {
					JsonObject que = new JsonObject();
					que.put(AJEntityBaseReports.ATTR_RESOURCE_ID, ky);
					que.put(AJEntityBaseReports.ATTR_LESSON_ID, val.getString(AJEntityBaseReports.LESSON_GOORU_OID));
					que.put(AJEntityBaseReports.ATTR_UNIT_ID, val.getString(AJEntityBaseReports.UNIT_GOORU_OID));
					que.put(AJEntityBaseReports.ATTR_COLLECTION_ID, val.getString(AJEntityBaseReports.COLLECTION_OID));
					que.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, val.getString(AJEntityBaseReports.COLLECTION_TYPE));	
					int studentCount = resourceStudentMap.get(ky) != null ? Integer.valueOf(resourceStudentMap.get(ky).toString()) : 0;
					que.put(JsonConstants.STUDENT_COUNT, studentCount);
					resultArray.add(que);					
				}
			});
		}
	} else {
		LOGGER.info("Questions pending grading cannot be obtained");
	}

  result.put(JsonConstants.GRADE_ITEMS, resultArray)
  .put(AJEntityBaseReports.ATTR_CLASS_ID, this.classId)
  .put(AJEntityBaseReports.ATTR_COURSE_ID, this.courseId);

  return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

}

  @Override
  public boolean handlerReadOnly() {
      return true;
  }

}
