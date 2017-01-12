package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

public class StudentAssessmentPerfHandler implements DBHandler {
	

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentAssessmentPerfHandler.class);
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
    private AJEntityClassAuthorizedUsers classAuthorizedUsers;
    
    private String classId;
    private String courseId;
    private String unitId;
    private String lessonId;
    private String collectionId;
    private String collectionType;
    private String userId;
    private String sessionId;

    private String collId;
    private String qtype;
    private String react;
    private String resourceTS;
    private String ansObj; 
    private String resType;
    private String resAttemptStatus;
    private String sco;
    private String SID;
    private String resViews;
    
    
    public StudentAssessmentPerfHandler(ProcessorContext context) {
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
      classAuthorizedUsers = new AJEntityClassAuthorizedUsers();
      if (context.getUserIdFromRequest() == null
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> creator = Base.findAll(classAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
        if (creator.isEmpty()) {
          List<Map> collaborator = Base.findAll(classAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
          if (collaborator.isEmpty()) {
            LOGGER.debug("validateRequest() FAILED");
            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
          }
        }
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> executeRequest() {

    	JsonObject resultBody = new JsonObject();
    	baseReport = new AJEntityBaseReports();

        this.userId = context.userIdFromSession();
        LOGGER.debug("UID is " + this.userId);
    	        
    	JsonArray sessionId_array = this.context.request().getJsonArray(REQUEST_SESSION_ID);
    	if (sessionId_array != null) {
    		LOGGER.debug("Session in Assessment Request is:" + sessionId_array.getString(0));
    		this.sessionId = sessionId_array.getString(0);
    	}
    	
    	JsonArray AssessmentKPIArray = new JsonArray();
    	    	    	
    	//STUDENT PERFORMANCE REPORTS IN ASSESSMENTS when SessionID NOT NULL
    	if (!sessionId.isEmpty()) {
        	List<Map> assessmentKPI = Base.findAll( AJEntityBaseReports.SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSIONID,
                    context.collectionId(), this.sessionId, AJEntityBaseReports.ATTR_EVENTNAME, 
                    AJEntityBaseReports.ATTR_ASSESSMENT, this.userId);
        	
        	if (!assessmentKPI.isEmpty()) {
        		LOGGER.debug("Assessment Attributes obtained");      	
            	
            	assessmentKPI.stream().forEach(m  -> { 
            			if (m.get(AJEntityBaseReports.EVENTTYPE).toString().equals(AJEntityBaseReports.ATTR_EVENTTYPE_START)){
            				resViews = m.get(AJEntityBaseReports.RESOURCE_VIEWS).toString();
            				LOGGER.debug("ResourceViews" + resViews);
            			} else if (m.get(AJEntityBaseReports.EVENTTYPE).toString().equals(AJEntityBaseReports.ATTR_EVENTTYPE_STOP)){
            				collId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
            				qtype = m.get(AJEntityBaseReports.QUESTION_TYPE).toString();
            				react = m.get(AJEntityBaseReports.REACTION).toString();
            				resourceTS =  m.get(AJEntityBaseReports.RESOURCE_TIMESPENT).toString();
            				//TODO - Mukul
            				//ansObj = m.get(AJEntityBaseReports.ANSWER_OBECT).toString();
            				//resType = m.get(AJEntityBaseReports.RESOURCE_TYPE).toString();
            				resAttemptStatus = m.get(AJEntityBaseReports.RESOURCE_ATTEMPT_STATUS).toString();
            				sco = m.get(AJEntityBaseReports.SCORE).toString();
            			}
            			if (m.get(AJEntityBaseReports.EVENTTYPE).toString().equals(AJEntityBaseReports.ATTR_EVENTTYPE_STOP)){
            				LOGGER.debug("Populating JSON array");
            				AssessmentKPIArray.add (new JsonObject().put(AJEntityBaseReports.COLLECTION_OID, collId)
            	            		.put(AJEntityBaseReports.QUESTION_TYPE, qtype)            		
            	            		.put(AJEntityBaseReports.REACTION, react)
            	            		.put(AJEntityBaseReports.RESOURCE_TIMESPENT, resourceTS)
            	            		.put(AJEntityBaseReports.RESOURCE_ATTEMPT_STATUS, resAttemptStatus)
            	            		.put(AJEntityBaseReports.SCORE, sco)
            	            		.put(AJEntityBaseReports.SESSION_ID, this.sessionId)
            	            		.put(AJEntityBaseReports.RESOURCE_VIEWS, resViews));
            				
            			}
            			          		
            	});

            } else {
                LOGGER.error("Assessment Attributes cannot be obtained");
                return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }
        	
        	LOGGER.info(AssessmentKPIArray.encodePrettily());
        	resultBody.put("UsageData:", AssessmentKPIArray);

        	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                    ExecutionStatus.SUCCESSFUL);
            		
        	} else {
                LOGGER.error("SessionID Missing, Cannot Obtain Student Lesson Perf data");
                return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }
    }   // End ExecuteRequest()
    

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
    	} else {
    		return false;
    	}
    		
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
