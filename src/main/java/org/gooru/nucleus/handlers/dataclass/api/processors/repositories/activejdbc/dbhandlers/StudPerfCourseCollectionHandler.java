package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
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

public class StudPerfCourseCollectionHandler implements DBHandler{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfCourseCollectionHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;
    private String collectionType;
    private String userId;
    private JsonArray collectionIds;

    
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
        JsonArray collectionArray = new JsonArray();

    	this.userId = this.context.request().getString(REQUEST_USERID);
        this.collectionIds = this.context.request().getJsonArray(MessageConstants.COLLECTION_IDS);
        LOGGER.debug("userId : {} - collectionIds:{}", userId, this.collectionIds);

        if (collectionIds.isEmpty()) {
          LOGGER.warn("CollectionIds are mandatory to fetch Student Performance in Assessments");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("CollectionIds are Missing. Cannot fetch Student Performance for Assessments"),
                  ExecutionStatus.FAILED);
        }

      this.userId = this.context.request().getString(REQUEST_USERID);
        
      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in an Assessment");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Assessment"),
                ExecutionStatus.FAILED);
  
      } 
  
      LOGGER.debug("UID is " + this.userId);

      //Populate collIds from the Context in API
      List<String> collIds = new ArrayList<>();
      for (Object s : this.collectionIds) {
          collIds.add(s.toString());
        }

        
        for (String collId : collIds) {        	
        	List<Map> collTSA = null;
        	LOGGER.debug("The collectionIds are" + collId);
        	JsonObject collectionKpi = new JsonObject();
            
        	//Find Timespent and Attempts
        	collTSA = Base.findAll(AJEntityBaseReports.GET_TOTAL_TIMESPENT_ATTEMPTS_FOR_COLLECTION, 
        			collId, this.userId, EventConstants.COLLECTION_PLAY);
        	
        	if (!collTSA.isEmpty()) {
        	collTSA.forEach(m -> {
        		collectionKpi.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString());
        		collectionKpi.put(AJEntityBaseReports.VIEWS, m.get(AJEntityBaseReports.VIEWS).toString());
	    		});
        	}
        		
        	collectionKpi.put(AJEntityBaseReports.COLLECTION_OID, collId);
        	collectionArray.add(collectionKpi);        		
        	}

        resultBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERUID, this.userId);
        //resultBody.put("PERF", "WORK IN PROGRESS");
      
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);  

      }       
      

    @Override
    public boolean handlerReadOnly() {
      return false;
    }

}
