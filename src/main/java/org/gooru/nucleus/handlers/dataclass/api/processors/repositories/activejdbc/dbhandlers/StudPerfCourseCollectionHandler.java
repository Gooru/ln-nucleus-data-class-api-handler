package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
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

public class StudPerfCourseCollectionHandler implements DBHandler {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfCourseCollectionHandler.class);
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;
    private int questionCount;
    private String userId;
    private String classId;
    private String courseId;
    private String unitId;
    private String lessonId;

    
    public StudPerfCourseCollectionHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch Student Performance in Assessments");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessments"),
                ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.getUserIdFromRequest() == null
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
        }
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
    	
    	//NOTE: This code will need to be refactored going ahead. (based on changes/updates to Student Performance Reports)
    	
        StringBuilder query = new StringBuilder(AJEntityBaseReports.GET_DISTINCT_COLLECTIONS);
        List<String> params = new ArrayList<>();
        JsonObject resultBody = new JsonObject();        
        JsonArray collectionArray = new JsonArray();

      this.userId = this.context.request().getString(REQUEST_USERID);
        
      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in a Collection");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);
  
      } else {
      	params.add(userId);        	
      }
      
      params.add(AJEntityBaseReports.ATTR_COLLECTION);
      
      
      this.classId = this.context.request().getString(MessageConstants.CLASS_ID);      
      if (StringUtil.isNullOrEmpty(classId)) {
          LOGGER.warn("ClassID is mandatory for fetching Student Performance in a Collection");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("Class Id Missing. Cannot fetch Student Performance in Collection"),
                  ExecutionStatus.FAILED);
    
        } else {
        	params.add(classId);        	
        }
      
      this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);      
      if (StringUtil.isNullOrEmpty(courseId)) {
          LOGGER.warn("CourseID is mandatory for fetching Student Performance in a Collection");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("Course Id Missing. Cannot fetch Student Performance in Collection"),
                  ExecutionStatus.FAILED);
    
        } else {
        	params.add(courseId);        	
        }

      this.unitId = this.context.request().getString(MessageConstants.UNIT_ID);
      if (!StringUtil.isNullOrEmpty(unitId)) {
    	  query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UNIT_ID);
    	  params.add(unitId);    
        } 
      
      this.lessonId = this.context.request().getString(MessageConstants.LESSON_ID);
      if (!StringUtil.isNullOrEmpty(lessonId)) {
    	  query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.LESSON_ID);
    	  params.add(lessonId);    
        } 
      
      LazyList<AJEntityBaseReports> collectionList = AJEntityBaseReports.findBySQL(query.toString(), params.toArray());
      
      //Populate collIds from the Context in API
      List<String> collIds = new ArrayList<>();
      if (!collectionList.isEmpty()) {          
          collectionList.forEach(c -> collIds.add(c.getString(AJEntityBaseReports.COLLECTION_OID)));
      }
      
        for (String collId : collIds) {        	
        	List<Map> collTSA = null;
        	JsonObject collectionKpi = new JsonObject();
            
        	//Find Timespent and Attempts
        	collTSA = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_COLLECTION, this.classId, this.courseId,
        			collId, AJEntityBaseReports.ATTR_COLLECTION, this.userId);
        	
        	if (!collTSA.isEmpty()) {
        	collTSA.forEach(m -> {
        		collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
        		collectionKpi.put(AJEntityBaseReports.VIEWS, Integer.parseInt(m.get(AJEntityBaseReports.VIEWS).toString()));
	    		});
        	}
        	
      	  
            List<Map> collectionQuestionCount = null;
            collectionQuestionCount = Base.findAll(AJEntityBaseReports.GET_COLLECTION_QUESTION_COUNT, this.classId, this.courseId,
                    collId, this.userId);

            //If questions are not present then Question Count is always zero, however this additional check needs to be added
            //since during migration of data from 3.0 chances are that QC may be null instead of zero
            collectionQuestionCount.forEach(qc -> {
            	if (qc.get(AJEntityBaseReports.QUESTION_COUNT) != null) {
            		this.questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
            	} else {
            		this.questionCount = 0;
            	}
            });
            double scoreInPercent = 0;
            if (this.questionCount > 0) {
              Object collectionScore = null;
              collectionScore = Base.firstCell(AJEntityBaseReports.GET_COLLECTION_SCORE, this.classId, this.courseId,
                      collId, this.userId);
              if (collectionScore != null) {
                scoreInPercent = (((Double.valueOf(collectionScore.toString())) / this.questionCount) * 100);
              }
              collectionKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(scoreInPercent));
            } else {
            	//If Collections have No Questions then score should be NULL
            	collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
            }            
            collectionKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);        	
            
            collectionArray.add(collectionKpi);

        	}

        resultBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERID, this.userId);
      
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);  

      }
    
    @Override
    public boolean handlerReadOnly() {
      return true;
    }

}
