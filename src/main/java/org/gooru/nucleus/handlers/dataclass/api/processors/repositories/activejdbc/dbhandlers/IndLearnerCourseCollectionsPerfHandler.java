package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class IndLearnerCourseCollectionsPerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerCourseCollectionsPerfHandler.class);
    private static final String REQUEST_USERID = "userId";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final ProcessorContext context;
    private String userId;
    private String courseId;
    private String unitId;
    private String lessonId;
    private String classId;

    
    public IndLearnerCourseCollectionsPerfHandler(ProcessorContext context) {
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
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
    	
    	//NOTE: This code will need to be refactored going ahead. (based on changes/updates to Student Performance Reports)
    	
        StringBuilder query = new StringBuilder(AJEntityBaseReports.GET_IL_COURSE_DISTINCT_COLLECTIONS);
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
      if(StringUtil.isNullOrEmpty(this.classId)){
        query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append("class_id IS NULL");
      } else{
        query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.CLASS_ID);
        params.add(classId);  
      }
      
      this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);      
      if (StringUtil.isNullOrEmpty(courseId)) {
          LOGGER.warn("CourseID is mandatory for fetching Student Performance in a Collection");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("Course Id Missing. Cannot fetch Student Performance in Collection"),
                  ExecutionStatus.FAILED);
    
        } else {
          query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.COURSE_ID);
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
      
      String startDate = this.context.request().getString(MessageConstants.START_DATE);
      if (!StringUtil.isNullOrEmpty(startDate)&&!isValidFormat(startDate)) {
        LOGGER.error("Invalid startDate");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid startDate. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);
  
      }
      if (!StringUtil.isNullOrEmpty(startDate)) {
        query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_GREATER_THAN_OR_EQUAL);
        params.add(startDate);
      }
      String endDate = this.context.request().getString(MessageConstants.END_DATE);
      if (!StringUtil.isNullOrEmpty(endDate)&&!isValidFormat(endDate)) {
        LOGGER.error("Invalid endDate");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid endDate. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);
  
      }
      if (!StringUtil.isNullOrEmpty(endDate)) {
        query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_LESS_THAN_OR_EQUAL);
        params.add(endDate);
      }
      LOGGER.debug("Query : " + query.toString());
      LazyList<AJEntityBaseReports> collectionList = AJEntityBaseReports.findBySQL(query.toString(), params.toArray());
      
      //Populate collIds from the Context in API
      List<String> collIds = new ArrayList<>();
      if (!collectionList.isEmpty()) {          
          collectionList.forEach(c -> collIds.add(c.getString(AJEntityBaseReports.COLLECTION_OID)));
      }
      
        for (String collId : collIds) {        	
          LazyList<AJEntityBaseReports> collTSA = null;
        	JsonObject collectionKpi = new JsonObject();
          List<String> collTSAParams = new ArrayList<>();
          collTSAParams.add(this.courseId);
          collTSAParams.add(collId);
          collTSAParams.add(AJEntityBaseReports.ATTR_COLLECTION);
          collTSAParams.add(this.userId);

          StringBuilder collTSAQuery = new StringBuilder(AJEntityBaseReports.GET_IL_COURSE_COLLECTION_PERF);
          if (!StringUtil.isNullOrEmpty(startDate)) {
            collTSAQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_GREATER_THAN_OR_EQUAL);
            collTSAParams.add(startDate);
          }
          if (!StringUtil.isNullOrEmpty(endDate)) {
            collTSAQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_LESS_THAN_OR_EQUAL);
            collTSAParams.add(endDate);
          }
          collTSAQuery.append(" GROUP BY collection_id");
          LOGGER.debug("collTSAQuery : " + collTSAQuery.toString());
        	//Find Timespent and Attempts
        	collTSA = AJEntityBaseReports.findBySQL(collTSAQuery.toString(),collTSAParams.toArray());
        	
        	if (!collTSA.isEmpty()) {
        	collTSA.forEach(m -> {
        		collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.TIME_SPENT).toString()));
        		collectionKpi.put(AJEntityBaseReports.VIEWS, Integer.parseInt(m.get(AJEntityBaseReports.VIEWS).toString()));
	    		});
        	}
        		
        	collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
        	collectionArray.add(collectionKpi);        		
        	}

        resultBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERUID, this.userId);
      
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);  

      }       
      
    public boolean isValidFormat(String value) {
      Date date = null;
      try {
        date = sdf.parse(value);
        if (!value.equals(sdf.format(date))) {
          date = null;
        }
      } catch (Exception ex) {
        LOGGER.error("Invalid date format...");
      }
      return date != null;
    }

    @Override
    public boolean handlerReadOnly() {
      return false;
    }

}
