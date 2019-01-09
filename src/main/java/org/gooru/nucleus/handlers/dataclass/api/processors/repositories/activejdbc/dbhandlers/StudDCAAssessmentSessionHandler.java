package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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

public class StudDCAAssessmentSessionHandler implements DBHandler {
	

	private static final Logger LOGGER = LoggerFactory.getLogger(StudDCAAssessmentSessionHandler.class);
	private static final String REQUEST_CLASS_ID = "classGooruId";
    private static final String REQUEST_OPEN_SESSION = "openSession";
    private static final String REQUEST_USERUID = "userUid";
    
    //private static final String REQUEST_USERID = "userId";
	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";
	private final ProcessorContext context;

    private String collectionId;
    private String sessionId;
    private Integer openSeq = 0;
    private Integer closedSeq = 0;

    boolean isStop = false;

    public StudDCAAssessmentSessionHandler(ProcessorContext context) {
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
    	
    	String sDate = this.context.request().getString(START_DATE);
    	String eDate = this.context.request().getString(END_DATE);      

        if (StringUtil.isNullOrEmpty(eDate) || StringUtil.isNullOrEmpty(sDate)) {
            LOGGER.warn("Date is mandatory to fetch Assessment Sessions in Daily Class Activity.");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
                    "Date Missing. Cannot fetch Assessment Sessions in Daily Class Activity"), ExecutionStatus.FAILED);

          }

        this.collectionId = context.collectionId();
        String classId = context.request().getString(EventConstants.CLASS_GOORU_OID);
        String openSession = this.context.request().getString(REQUEST_OPEN_SESSION);
        String userId = this.context.request().getString(REQUEST_USERUID);
        
        LOGGER.debug("classId :{} , userId : {} and collectionId:{}", classId, userId, this.collectionId);
    	
        if (StringUtil.isNullOrEmpty(openSession)) {
    		openSession = "false";
            LOGGER.info("By Default OpenSession is assumed to be false");
        }

        if (StringUtil.isNullOrEmpty(classId)) {
            LOGGER.warn("ClassID is mandatory to fetch Session Information");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("ClassId Missing. Cannot fetch Session Information"),
                ExecutionStatus.FAILED);
        }

        
        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is mandatory to fetch Session Information");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("UserID Missing. Cannot fetch Session Information"),
                ExecutionStatus.FAILED);
        }
        
        Date startDate = Date.valueOf(sDate);
        Date endDate = Date.valueOf(eDate);
        
        List<Map> distinctSessionsList;
          distinctSessionsList = Base.findAll( AJEntityDailyClassActivity.GET_ASMT_USER_SESSIONS_FOR_COLLID, classId, 
        		  this.collectionId, userId, startDate, endDate);
                
    	if (!distinctSessionsList.isEmpty()) {
    		distinctSessionsList.forEach(m -> {
        		sessionId = m.get(AJEntityDailyClassActivity.SESSION_ID).toString();
        		LOGGER.debug(sessionId);
        		isStop = false;
        		JsonObject sessionObj = new JsonObject();

        		List<Map> sessionStatusMap = Base.findAll( AJEntityDailyClassActivity.GET_SESSION_STATUS,
        	   			 sessionId, this.collectionId, EventConstants.COLLECTION_PLAY);

        		if (!sessionStatusMap.isEmpty()){

        			sessionStatusMap.forEach(sess -> {
        				if (Objects.equals(sess.get(AJEntityDailyClassActivity.EVENTTYPE).toString(), EventConstants.START)){
        	   				sessionObj.put(JsonConstants.EVENT_TIME, sess.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString())
        	   				.put(JsonConstants.SESSIONID, sessionId);
        	   			}

        				if (Objects.equals(sess.get(AJEntityDailyClassActivity.EVENTTYPE).toString(), EventConstants.STOP)){
        					//Specific Change related to Migration (3.0 -> 4.0)
        					if (!sessionObj.isEmpty()) {
        						sessionObj.clear();
        					}
        					closedSeq++;
        					sessionObj.put(JsonConstants.EVENT_TIME, sess.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString())
        	   				.put(JsonConstants.SESSIONID, sessionId);
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



    	}else{
    	  LOGGER.debug("No sessions data found for given assessment");
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
        return true;
    }

}
