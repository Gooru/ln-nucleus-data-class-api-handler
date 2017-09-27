package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class UserAssessmentSessionsHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(UserAssessmentSessionsHandler.class);

	private static final String REQUEST_CLASS_ID = "classGooruId";
    private static final String REQUEST_COURSE_ID = "courseGooruId";
    private static final String REQUEST_UNIT_ID = "unitGooruId";
    private static final String REQUEST_LESSON_ID = "lessonGooruId";
    private static final String REQUEST_OPEN_SESSION = "openSession";
    private static final String REQUEST_USERID = "userUid";

	private final ProcessorContext context;

    private String collectionId;
    private String sessionId;
    private Integer openSeq = 0;
    private Integer closedSeq = 0;

    boolean isStop = false;

    public UserAssessmentSessionsHandler(ProcessorContext context) {
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

        AJEntityBaseReports baseReport = new AJEntityBaseReports();

        this.collectionId = context.collectionId();
        String classId = context.request().getString(EventConstants.CLASS_GOORU_OID);
        String courseId = context.request().getString(EventConstants.COURSE_GOORU_OID);
        String unitId = context.request().getString(EventConstants.UNIT_GOORU_OID);
        String lessonId = context.request().getString(EventConstants.LESSON_GOORU_OID);
        String openSession = this.context.request().getString(REQUEST_OPEN_SESSION);
        String userId = this.context.request().getString(REQUEST_USERID);
       LOGGER.debug("classId :{} , userId : {} and collectionId:{}", classId, userId, this.collectionId);
    	if (StringUtil.isNullOrEmpty(openSession)) {
    		openSession = "false";
            LOGGER.info("By Default OpenSession is assumed to be false");
        }

        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is mandatory to fetch Session Information");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("UserID Missing. Cannot fetch Session Information"),
                ExecutionStatus.FAILED);
        }
        List<Map> distinctSessionsList;
        if(!StringUtil.isNullOrEmpty(classId)){
          distinctSessionsList = Base.findAll( AJEntityBaseReports.GET_USER_SESSIONS_FOR_COLLID, classId, courseId,


              unitId,


              lessonId,
    			this.collectionId, EventConstants.ASSESSMENT, userId);
        }else{
          distinctSessionsList = Base.findAll( AJEntityBaseReports.GET_USER_SESSIONS_FOR_COLLID_,
                  this.collectionId, EventConstants.ASSESSMENT, userId);
        }
    	if (!distinctSessionsList.isEmpty()) {
    		distinctSessionsList.forEach(m -> {
        		sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
        		LOGGER.debug(sessionId);
        		isStop = false;
        		JsonObject sessionObj = new JsonObject();

        		List<Map> sessionStatusMap = Base.findAll( AJEntityBaseReports.GET_SESSION_STATUS,
        	   			 sessionId, this.collectionId, EventConstants.COLLECTION_PLAY);

        		if (!sessionStatusMap.isEmpty()){

        			sessionStatusMap.forEach(sess -> {
        				if (Objects.equals(sess.get(AJEntityBaseReports.EVENTTYPE).toString(), EventConstants.START)){
        	   				sessionObj.put(JsonConstants.EVENT_TIME, sess.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString())
        	   				.put(JsonConstants.SESSIONID, sessionId);

        	   			}

        				if (Objects.equals(sess.get(AJEntityBaseReports.EVENTTYPE).toString(), EventConstants.STOP)){
        					//Specific Change related to Migration (3.0 -> 4.0)
        					if (!sessionObj.isEmpty()) {
        						sessionObj.clear();
        					}
        					closedSeq++;
        					sessionObj.put(JsonConstants.EVENT_TIME, sess.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString())
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
