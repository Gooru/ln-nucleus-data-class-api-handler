package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
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

/**
 * Created by mukul@gooru
 */
public class UserCollectionSessionsHandler implements DBHandler {
	

	private static final Logger LOGGER = LoggerFactory.getLogger(UserAssessmentSessionsHandler.class);
	
	private static final String REQUEST_CLASS_ID = "classGooruId";
    private static final String REQUEST_COURSE_ID = "courseGooruId";
    private static final String REQUEST_UNIT_ID = "unitGooruId";
    private static final String REQUEST_LESSON_ID = "lessonGooruId";
    private static final String REQUEST_OPEN_SESSION = "openSession";
    private static final String REQUEST_USERID = "userUid";
    
	private final ProcessorContext context;
    private AJEntityBaseReports baseReport;

    private String classId;
    private String courseId;
    private String unitId;
    private String lessonId;
    private String collectionId;
    private String userId;
    private String sessionId;
    private Integer openSeq = new Integer(0);
    private Integer closedSeq = new Integer(0);
    
    private String openSession = new String("false");
    boolean isStop = false;
        
    public UserCollectionSessionsHandler(ProcessorContext context) {
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
    	LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
    	
    	JsonObject resultBody = new JsonObject();       	    	
    	JsonArray closedSessionArray = new JsonArray();
    	JsonArray openSessionArray = new JsonArray();
    	    	
    	baseReport = new AJEntityBaseReports();
    
        this.collectionId = context.collectionId();    	
        
      	this.openSession = this.context.request().getString(REQUEST_OPEN_SESSION);
      	if (StringUtil.isNullOrEmpty(openSession)) {
      		this.openSession = "false";
              LOGGER.info("By Default OpenSession is assumed to be false");            
          }
          
          this.userId = this.context.request().getString(REQUEST_USERID);
          if (StringUtil.isNullOrEmpty(userId)) {
              LOGGER.warn("UserID is mandatory to fetch Session Information");
              return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("UserID Missing. Cannot fetch Session Information"),
                  ExecutionStatus.FAILED);
          }
          LOGGER.debug("UID is " + this.userId);

    	List<Map> distinctSessionsList = Base.findAll( AJEntityBaseReports.GET_USER_SESSIONS_FOR_COLLID, 
    			this.collectionId, EventConstants.COLLECTION, this.userId);
    	if (!distinctSessionsList.isEmpty()) {
    		
    		distinctSessionsList.forEach(m -> {    		
        		sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
        		LOGGER.debug(sessionId.toString());        		
        		isStop = false;
        		JsonObject sessionObj = new JsonObject();
        		
        		List<Map> sessionStatusMap = Base.findAll( AJEntityBaseReports.GET_SESSION_STATUS, 
        	   			 sessionId, this.collectionId, EventConstants.COLLECTION_PLAY);
        	   	 
        		//TODO: Covert TimeStamp, also ORDER BY DESC so that the higher sequence number will mean latest sessions
        		if (!sessionStatusMap.isEmpty()){
        			
        			sessionStatusMap.forEach(sess -> {
        				if (sess.get(EventConstants.EVENT_TYPE).toString().equals(EventConstants.START)){
        	   				sessionObj.put(JsonConstants.EVENT_TIME, sess.get(AJEntityBaseReports.CREATE_TIMESTAMP).toString())
        	   				.put(JsonConstants.SESSIONID, sessionId);
        	   						
        	   			}
        	   			
        				if (sess.get(EventConstants.EVENT_TYPE).toString().equals(EventConstants.STOP)){
        					closedSeq++;
        					sessionObj.put(JsonConstants.SEQUENCE, closedSeq.toString());
        	   				isStop = true;
        					closedSessionArray.add(sessionObj);
        	   			}
        	    		});
        			
        			if (!isStop) {
        				openSeq++;
        				sessionObj.put(JsonConstants.SEQUENCE, openSeq.toString());
        				openSessionArray.add(sessionObj);
        			}
        		} 
        	});

    	
    		
    	}
    	    	
    	if (openSession.equalsIgnoreCase("false")) {
    		resultBody.put(JsonConstants.CONTENT, closedSessionArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);    		
    	} else if (openSession.equalsIgnoreCase("true")) {
    		resultBody.put(JsonConstants.CONTENT, openSessionArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
    	}

    	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                ExecutionStatus.SUCCESSFUL);
    	
    }   
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
    
    private boolean validateOptionalParams(ProcessorContext context) {
    	

    	JsonArray classId_array = this.context.request().getJsonArray(REQUEST_CLASS_ID);
    	JsonArray courseId_array = this.context.request().getJsonArray(REQUEST_COURSE_ID);
    	JsonArray unitId_array = this.context.request().getJsonArray(REQUEST_UNIT_ID);
    	JsonArray lessonId_array = this.context.request().getJsonArray(REQUEST_LESSON_ID);
    	
    	if ((classId_array != null) && (courseId_array != null) && (unitId_array != null) && (lessonId_array != null)){
    		this.classId = classId_array.getString(0);
    		this.courseId = courseId_array.getString(0);
    		this.unitId = unitId_array.getString(0);
    		this.lessonId = lessonId_array.getString(0);
    		
    		return true;    		
    	} else return false;
    }
 
}
