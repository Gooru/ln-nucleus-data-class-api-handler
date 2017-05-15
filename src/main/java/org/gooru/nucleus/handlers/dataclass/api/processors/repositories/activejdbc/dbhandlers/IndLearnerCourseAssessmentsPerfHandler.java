package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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

public class IndLearnerCourseAssessmentsPerfHandler implements DBHandler {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerCourseAssessmentsPerfHandler.class);
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;
    private String userId;    
    private String courseId;
    private String unitId;
    private String lessonId;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    
    public IndLearnerCourseAssessmentsPerfHandler(ProcessorContext context) {
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
        JsonArray assessmentArray = new JsonArray();

      this.userId = this.context.request().getString(REQUEST_USERID);
        
      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in a Collection");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);
  
      } else {
      	params.add(userId);        	
      }
      
      params.add(AJEntityBaseReports.ATTR_ASSESSMENT);          
      
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
    	  query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UNIT_ID);
    	  params.add(unitId);    
        } 
      
      this.lessonId = this.context.request().getString(MessageConstants.LESSON_ID);
      if (!StringUtil.isNullOrEmpty(lessonId)) {
    	  query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.LESSON_ID);
    	  params.add(lessonId);    
        } 
      String startDate = this.context.request().getString(MessageConstants.START_DATE);
    
      if (!StringUtil.isNullOrEmpty(startDate)&&!isValidFormat(startDate)) {
        LOGGER.error("Invalid startDate");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid startDate. Cannot fetch Student Performance in Assessment"),
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
                MessageResponseFactory.createInvalidRequestResponse("Invalid endDate. Cannot fetch Student Performance in Assessment"),
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
        	List<Map> assessScore = null;
        	LazyList<AJEntityBaseReports> assessTSA = null;
        	LOGGER.debug("The collectionIds are" + collId);
        	JsonObject assessmentKpi = new JsonObject();
          List<String> assessTSAParams = new ArrayList<>();
          
          StringBuilder assessTSAQuery = new StringBuilder(AJEntityBaseReports.GET_IL_COURSE_ASSESSMENTS_TOTAL_TIME_SPENT_ATTEMPTS);
          assessTSAParams.add(this.courseId);
          assessTSAParams.add(collId);
          assessTSAParams.add(AJEntityBaseReports.ATTR_ASSESSMENT);
          assessTSAParams.add(this.userId);
          assessTSAParams.add(EventConstants.COLLECTION_PLAY);
          assessTSAParams.add(EventConstants.STOP);
          if (!StringUtil.isNullOrEmpty(startDate)) {
            assessTSAQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_GREATER_THAN_OR_EQUAL);
            assessTSAParams.add(startDate);
          }
          if (!StringUtil.isNullOrEmpty(endDate)) {
            assessTSAQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_LESS_THAN_OR_EQUAL);
            assessTSAParams.add(endDate);
          }
          assessTSAQuery.append(" GROUP BY collection_id");
          LOGGER.debug("assessTSA Query : ", assessTSAQuery.toString());
          //Find Timespent and Attempts
        	assessTSA = AJEntityBaseReports.findBySQL(assessTSAQuery.toString(), assessTSAParams.toArray());
        	
        	if (!assessTSA.isEmpty()) {
        	assessTSA.forEach(m -> {
        		assessmentKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.TIME_SPENT).toString()));
        		assessmentKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, Integer.parseInt(m.get(AJEntityBaseReports.VIEWS).toString()));
	    		});
        	}
        	
        	StringBuilder assessScoreQuery = new StringBuilder(AJEntityBaseReports.GET_IL_COURSE_ASSESSMENTS_SCORE);
        	
        	 if (!StringUtil.isNullOrEmpty(startDate)) {
        	   assessScoreQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_GREATER_THAN_OR_EQUAL);
           }
           if (!StringUtil.isNullOrEmpty(endDate)) {
             assessScoreQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_LESS_THAN_OR_EQUAL);
           }
           LOGGER.debug("assessScore Query : ", assessTSAQuery.toString());
        	//Get the latest Score
        	assessScore = Base.findAll(assessScoreQuery.toString(),assessTSAParams.toArray());
        	
        	if (!assessScore.isEmpty()){
        		assessScore.forEach(m -> {
            		assessmentKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(Double.valueOf((m.get(AJEntityBaseReports.SCORE).toString()))));
            		assessmentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);        
    	    		});
            	}
        		
        	assessmentKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
        	assessmentArray.add(assessmentKpi);        		
        	}

      resultBody.put(JsonConstants.USAGE_DATA, assessmentArray).put(JsonConstants.USERUID, this.userId);
      
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
