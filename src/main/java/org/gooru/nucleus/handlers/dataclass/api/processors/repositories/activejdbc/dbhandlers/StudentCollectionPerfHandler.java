package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;

import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

public class StudentCollectionPerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(StudentCollectionPerfHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userUid";
    
    private static final String REQUEST_CLASS_ID = "classGooruId";
    private static final String REQUEST_COURSE_ID = "courseGooruId";
    private static final String REQUEST_UNIT_ID = "unitGooruId";
    private static final String REQUEST_LESSON_ID = "lessonGooruId";
    private static final String REQUEST_COLLECTION_ID = "collectionId";
    private static final String REQUEST_SESSION_ID = "sessionId";
        
	private final ProcessorContext context;
    private AJEntityBaseReports baseReport;
     
    private String classId;
    private String courseId;
    private String unitId;
    private String lessonId;
    private String collectionId;
    private String collectionType;
    private String userId;
    private String sessionId;

    String collId;
    String qtype;
    String react;
    String resourceTS;
    String ansObj; 
    String resType;
    String resAttemptStatus;
    String sco;
    String SID;
    String resViews;


    public StudentCollectionPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Assessments");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Units"),
                ExecutionStatus.FAILED);
        }

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
    	baseReport = new AJEntityBaseReports();
    	
    	JsonArray sessionId_array = this.context.request().getJsonArray(REQUEST_SESSION_ID);
    	if (sessionId_array != null) {    		
    		this.sessionId = sessionId_array.getString(0);
    	}
    	
    	JsonArray CollectionKPIArray = new JsonArray();
    	//STUDENT PERFORMANCE REPORTS IN COLLECTION when SessionID NOT NULL
    	if (!sessionId.isEmpty()) {
        	List<Map> collectionKPI = Base.findAll( AJEntityBaseReports.SELECT_COLLECTION_FOREACH_COLLID_AND_SESSIONID,
                    context.collectionId(), this.sessionId, AJEntityBaseReports.ATTR_EVENTNAME, 
                    AJEntityBaseReports.ATTR_COLLECTION, this.userId);
        	
        	if (!collectionKPI.isEmpty()) {
        		LOGGER.debug("Collection Attributes obtained");      	

            	collectionKPI.stream().forEach(m  -> { 
            			if (m.get(AJEntityBaseReports.EVENTTYPE).toString().equals(AJEntityBaseReports.ATTR_EVENTTYPE_START)){
            				resViews = m.get(AJEntityBaseReports.RESOURCE_VIEWS).toString();
            				LOGGER.debug("ResourceViews" + resViews);
            			} else if (m.get(AJEntityBaseReports.EVENTTYPE).toString().equals(AJEntityBaseReports.ATTR_EVENTTYPE_STOP)){
            				collId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();            				
            				react = m.get(AJEntityBaseReports.REACTION).toString();
            				resourceTS =  m.get(AJEntityBaseReports.RESOURCE_TIMESPENT).toString();
            				//These will only be populated when the Collection has a few Questions in it
            				qtype = m.get(AJEntityBaseReports.QUESTION_TYPE).toString();
            				//ansObj = m.get(AJEntityBaseReports.ANSWER_OBECT).toString();
            				//resType = m.get(AJEntityBaseReports.RESOURCE_TYPE).toString();  
            				resAttemptStatus = m.get(AJEntityBaseReports.RESOURCE_ATTEMPT_STATUS).toString();
            				sco = m.get(AJEntityBaseReports.SCORE).toString();
            			}
            			if (m.get(AJEntityBaseReports.EVENTTYPE).toString().equals(AJEntityBaseReports.ATTR_EVENTTYPE_STOP)){
            				LOGGER.debug("Populating JSON array");
            				CollectionKPIArray.add (new JsonObject().put(AJEntityBaseReports.COLLECTION_OID, collId)            	            		            		
            	            		.put(AJEntityBaseReports.REACTION, react)
            	            		.put(AJEntityBaseReports.RESOURCE_TIMESPENT, resourceTS)            	            		
            	            		.put(AJEntityBaseReports.SESSION_ID, this.sessionId)
            	            		.put(AJEntityBaseReports.RESOURCE_VIEWS, resViews)
            	            		.put(AJEntityBaseReports.QUESTION_TYPE, qtype)
            	            		.put(AJEntityBaseReports.RESOURCE_ATTEMPT_STATUS, resAttemptStatus)
            	            		.put(AJEntityBaseReports.SCORE, sco));            	                   				
            			}
            			          		
            	});

            }
        	
        	resultBody.put("UsageData:", CollectionKPIArray);

        	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                    ExecutionStatus.SUCCESSFUL);
            		
        	} else {
                LOGGER.error("SessionId not found. Performance Data cannot be obtained");
                return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }
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
