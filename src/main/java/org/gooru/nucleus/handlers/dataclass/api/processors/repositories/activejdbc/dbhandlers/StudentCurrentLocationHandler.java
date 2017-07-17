package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class StudentCurrentLocationHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudentCurrentLocationHandler.class);
	    
	  private final ProcessorContext context;
    private AJEntityBaseReports baseReport;

    private String classId;
    private String userId;
        
    public StudentCurrentLocationHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
    	
    	//No Sanity Check required since, no params are being passed in Request Body
 
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest())) {
        LOGGER.debug("User ID in the session : {}", context.userIdFromSession());
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
        }
        LOGGER.debug("User is teacher of this class.");
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
    	
    	JsonObject resultBody = new JsonObject();
       	
    	JsonArray CurrentLocArray = new JsonArray();
    	baseReport = new AJEntityBaseReports();
    
        this.classId = context.classId();    	
        this.userId = context.getUserIdFromRequest();     

    	List<Map> CurrentLocMap = Base.findAll( AJEntityBaseReports.GET_STUDENT_LOCATION,this.classId, this.userId);
    	
    	if (!CurrentLocMap.isEmpty()){
    	  CurrentLocMap.forEach(m -> {
          JsonObject loc = new JsonObject();
          loc.put(AJEntityBaseReports.ATTR_CLASS_ID, m.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
          loc.put(AJEntityBaseReports.ATTR_COURSE_ID, m.get(AJEntityBaseReports.COURSE_GOORU_OID) != null ? m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString() :null);
          loc.put(AJEntityBaseReports.ATTR_UNIT_ID, m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null);
          loc.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null);
          String collId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
          if (m.get(AJEntityBaseReports.COLLECTION_TYPE).equals(EventConstants.ASSESSMENT)) {
            loc.put(AJEntityBaseReports.ATTR_ASSESSMENT_ID, collId);
        	Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
            loc.put(JsonConstants.ASSESSMENT_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
          } else {
            loc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.COLLECTION_OID).toString());
        	Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
            loc.put(JsonConstants.COLLECTION_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
          }
          CurrentLocArray.add(loc);
        });
      
    	} else {            
            LOGGER.info("Current Location Attributes cannot be obtained");
        }

        //Form the required JSon pass it on
        resultBody.put(JsonConstants.CONTENT, CurrentLocArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
                 
    	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                ExecutionStatus.SUCCESSFUL);
    	
    }   
    

    @Override
    public boolean handlerReadOnly() {
        return true;
    } 
}
