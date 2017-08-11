package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
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

public class StudentAnsForRubricQuesHandler implements DBHandler {
	
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentAnsForRubricQuesHandler.class);	  
	  private final ProcessorContext context;
	  private AJEntityBaseReports baseReport;
	  
	  private String classId;
	  private String courseId;
	  private String collectionId;
	  private String userId;
	  
	  
	  public StudentAnsForRubricQuesHandler(ProcessorContext context) {
	      this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {
	      if (context.request() == null || context.request().isEmpty()) {
	          LOGGER.warn("Invalid request received to fetch Student Answer for Question");
	          return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Answer for Question"),
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
	  baseReport = new AJEntityBaseReports();

	  this.classId = this.context.request().getString(MessageConstants.CLASS_ID);      
	  if (StringUtil.isNullOrEmpty(classId)) {
	      LOGGER.warn("ClassID is mandatory to fetch student's answer to grade");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Class Id Missing. Cannot fetch student's answer"),
	              ExecutionStatus.FAILED);

	    } 
	  
	  this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);      
	  if (StringUtil.isNullOrEmpty(courseId)) {
	      LOGGER.warn("CourseID is mandatory to fetch student's answer to grade");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Course Id Missing. Cannot fetch student's answer"),
	              ExecutionStatus.FAILED);

	    } 
	  
	  this.collectionId = this.context.request().getString(MessageConstants.COLLECTION_ID);      
	  if (StringUtil.isNullOrEmpty(collectionId)) {
	      LOGGER.warn("CollectionID is mandatory to fetch student's answer to grade");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Collection Id Missing. Cannot fetch student's answer"),
	              ExecutionStatus.FAILED);

	    } 

		List<Map> ansMap = Base.findAll(AJEntityBaseReports.GET_STUDENTS_ANSWER_FOR_RUBRIC_QUESTION, 
				this.classId, this.courseId, this.collectionId, context.questionId(), context.studentId());
		
		if (!ansMap.isEmpty()){
			  ansMap.forEach(m -> {		        
		    result.put(AJEntityBaseReports.ATTR_COURSE_ID, this.courseId);		    
		    result.put(AJEntityBaseReports.ATTR_COLLECTION_ID, this.collectionId );
		    result.put(AJEntityBaseReports.ATTR_QUESTION_ID, context.questionId());
		    result.put(AJEntityBaseReports.ATTR_QUESTION_TEXT, "NA");
		    result.put(AJEntityBaseReports.ATTR_ANSWER_TEXT, m.get(AJEntityBaseReports.ATTR_ANSWER_TEXT) != null ? 
		    		m.get(AJEntityBaseReports.ATTR_ANSWER_TEXT).toString() : null);
		    result.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
		    result.put(AJEntityBaseReports.SUBMITTED_AT, (m.get(AJEntityBaseReports.SUBMITTED_AT).toString()));
		    result.put(AJEntityBaseReports.SESSION_ID, (m.get(AJEntityBaseReports.SESSION_ID).toString()));
		  });

			} else {            
		      LOGGER.info("Questions pending grading cannot be obtained");
		  }


	  //result.put(JsonConstants.STUDENTS , "Getting Answers");
	  return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

	}   

	  @Override
	  public boolean handlerReadOnly() {
	      return true;
	  }

}
