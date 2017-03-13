package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudPerfMultipleAssessmentHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfMultipleAssessmentHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;    
    private String userId;
    private String classId;
    private JsonArray collectionIds;

    
    public StudPerfMultipleAssessmentHandler(ProcessorContext context) {
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
      //FIXME: to be reverted
     /* if (context.getUserIdFromRequest() == null
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> creator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
        if (creator.isEmpty()) {
          List<Map> collaborator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_COLLABORATOR, this.context.classId(), this.context.userIdFromSession());
          if (collaborator.isEmpty()) {
            LOGGER.debug("validateRequest() FAILED");
            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
          }
        }
      }*/
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
   
    	JsonObject resultBody = new JsonObject();        
        JsonArray assessmentArray = new JsonArray();
        
    	this.userId = this.context.request().getString(REQUEST_USERID);
        this.collectionIds = this.context.request().getJsonArray(MessageConstants.COLLECTION_IDS);
        LOGGER.debug("userId : {} - collectionIds:{}", userId, this.collectionIds);

        if (collectionIds.isEmpty()) {
          LOGGER.warn("CollectionIds are mandatory to fetch Student Performance in Assessments");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("CollectionIds are Missing. Cannot fetch Student Performance for Assessments"),
                  ExecutionStatus.FAILED);
        }


        List<String> collIds = new ArrayList<>();
        for (Object s : this.collectionIds) {
            collIds.add(s.toString());
          }
           
      List<Map> assessmentPerf = null;  

      this.userId = this.context.request().getString(REQUEST_USERID);
      
      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in an Assessment");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Assessment"),
                ExecutionStatus.FAILED);
  
      }

      this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
      if (!StringUtil.isNullOrEmpty(classId)) {
        LOGGER.debug("Fetching Performance for Assessments in Class");
        assessmentPerf = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS, classId,
                listToPostgresArrayString(collIds), userId, EventConstants.COLLECTION_PLAY);  
      } else {
          LOGGER.debug("Fetching Performance for Assessments outside Class");
          assessmentPerf = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_ASSESSMENTS,
                  listToPostgresArrayString(collIds), userId, EventConstants.COLLECTION_PLAY);  
    	  
      }
      
      if (!assessmentPerf.isEmpty()) {
          assessmentPerf.forEach(m -> {
        	JsonObject assessmentKpi = new JsonObject();
      		assessmentKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
        	assessmentKpi.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString());
      		assessmentKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString());
      		assessmentKpi.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE).toString());
    		assessmentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
    		
    		assessmentArray.add(assessmentKpi);
              
      });
                  		
    	  
      } else {
    	  LOGGER.debug("No data available for ANY of the Assessments passed on to this endpoint");
      }
       /** for (String collId : collIds) {
        	List<Map> assessScore = null;
        	List<Map> assessTSA = null;
        	LOGGER.debug("The collectionIds are" + collId);
        	JsonObject assessmentKpi = new JsonObject();
            
        	//Find Timespent and Attempts
        	assessTSA = Base.findAll(AJEntityBaseReports.GET_TOTAL_TIMESPENT_ATTEMPTS_FOR_ASSESSMENT, 
        			collId, AJEntityBaseReports.ATTR_ASSESSMENT, this.userId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
        	
        	if (!assessTSA.isEmpty()) {
        	assessTSA.forEach(m -> {
        		assessmentKpi.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString());
        		assessmentKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString());
	    		});
        	}
        	
        	//Get the latest Score
        	assessScore = Base.findAll(AJEntityBaseReports.GET_LATEST_SCORE_FOR_ASSESSMENT, 
            		collId, AJEntityBaseReports.ATTR_ASSESSMENT, this.userId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
        	
        	if (!assessScore.isEmpty()){
        		assessScore.forEach(m -> {
            		assessmentKpi.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE).toString());
            		assessmentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);        
    	    		});
            	}
        		
        	assessmentKpi.put(AJEntityBaseReports.COLLECTION_OID, collId);
        	assessmentArray.add(assessmentKpi);        		
        	} **/

      resultBody.put(JsonConstants.USAGE_DATA, assessmentArray).put(JsonConstants.USERUID, this.userId);
      
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);  

      }       
      

    @Override
    public boolean handlerReadOnly() {
      return false;
    }

    private String listToPostgresArrayString(List<String> input) {
        int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
                                                    // 36
                                                    // chars
        Iterator<String> it = input.iterator();
        if (!it.hasNext()) {
          return "{}";
        }
    
        StringBuilder sb = new StringBuilder(approxSize);
        sb.append('{');
        for (;;) {
          String s = it.next();
          sb.append('"').append(s).append('"');
          if (!it.hasNext()) {
            return sb.append('}').toString();
          }
          sb.append(',');
        }
    
      }
}
