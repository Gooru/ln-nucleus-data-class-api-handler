package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.formatter.*;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;

import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.StudentCoursePerfHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

/**
 * Created by mukul@gooru
 */
class StudentCoursePerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(StudentCoursePerfHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userUid";
    
	  private final ProcessorContext context;
    private AJEntityBaseReports baseReport;
    private AJEntityClassAuthorizedUsers classAuthorizedUsers;

    
    private String collectionType;
    private String userId;
    
    //For stuffing Json
    private String unitId;
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
    private String compCount;

    public StudentCoursePerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in course"),
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
      LOGGER.debug("User ID in the session : {}", context.userIdFromSession());
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
    	JsonObject contentBody = new JsonObject();
    	JsonArray resultarray = new JsonArray();
    	baseReport = new AJEntityBaseReports();
    	
    	//CollectionType is a Mandatory Parameter
    	this. collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
    	if (StringUtil.isNullOrEmpty(collectionType)) {
            LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in course"),
                ExecutionStatus.FAILED);
        }
        
        this.userId = this.context.request().getString(REQUEST_USERID);
        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is mandatory to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("UserID Missing. Cannot fetch Student Performance in course"),
                ExecutionStatus.FAILED);
        }        
        
        List<String> unitIds = new ArrayList<>();
        JsonArray CourseKpiArray = new JsonArray();
        
        //If CollectionType is Assessment
        if (collectionType.equals(EventConstants.ASSESSMENT)) {
        	LazyList<AJEntityBaseReports> unitIDforCourse = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_UNITID_FOR_COURSEID_FILTERBY_COLLTYPE,
                    context.classId(), context.courseId(), this.collectionType, this.userId);
        	
        	if (!unitIDforCourse.isEmpty()) {
        		LOGGER.debug("Got a list of Distinct unitIDs for this Course");
            	
            	unitIDforCourse.forEach(unit -> unitIds.add(unit.getString(AJEntityBaseReports.UNIT_GOORU_OID)));
                
                List<Map> completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT_FOREACH_UNITID,
                		context.classId(), context.courseId(), this.collectionType, this.userId, EventConstants.COLLECTION_PLAY, AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
                                
                if(completedCountMap.isEmpty()){
                	LOGGER.debug("Error in your getCount Query, No data returned");
                	return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                }
                        
                List<Map> assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_COURSE_PERF_FOR_ASSESSMENT,
                		listToPostgresArrayString(unitIds), this.collectionType, this.userId);
              
                if(assessmentKpi.isEmpty()){
                	LOGGER.debug("No data returned for Student Perf in Assessment");
                	return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                }
                
                assessmentKpi.forEach(m -> { 
                	unitId = m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString();
                	LOGGER.debug("The Value of UNITID " + unitId);
                	completedCountMap.forEach(map -> { if ((map.get(AJEntityBaseReports.UNIT_GOORU_OID).toString()).equals(unitId)) {
                		compCount = map.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString();
                	}
                	});                	
                	CourseKpiArray.add(new JsonObject().put(AJEntityBaseReports.UNIT_GOORU_OID, unitId)
                    		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())
                    		.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, compCount)
                    		.put(AJEntityBaseReports.ATTR_ATTEMPT_STATUS, AJEntityBaseReports.NA)
                    		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                    		.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE).toString())
                    		.put(AJEntityBaseReports.ATTR_ATTEMPTS, m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString())
                    		.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, AJEntityBaseReports.NA));                	
                });                
                
        	} else {
                LOGGER.error("Could not get Student Course Performance");
                return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }
        }
    
        //If collection is collection
        if (collectionType.equals(EventConstants.COLLECTION)) {
        	LazyList<AJEntityBaseReports> unitIDforCourse = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_UNITID_FOR_COURSEID_FILTERBY_COLLTYPE,
                    context.classId(), context.courseId(), this.collectionType, this.userId);
        	
        	if (!unitIDforCourse.isEmpty()) {
        		LOGGER.debug("Got a list of Distinct unitIDs for this Course");
        
                unitIDforCourse.forEach(unit -> unitIds.add(unit.getString(AJEntityBaseReports.UNIT_GOORU_OID)));

                List<Map> completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT_FOREACH_UNITID,
                		context.classId(), context.courseId(), this.collectionType, this.userId, EventConstants.COLLECTION_PLAY, AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
                                
                if(completedCountMap.isEmpty()){
                	LOGGER.debug("Error in your getCount Query, No data returned");
                	return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                }
                        
                List<Map> collectionKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION,
                		listToPostgresArrayString(unitIds), this.collectionType, this.userId);
                
                if(collectionKpi.isEmpty()){
                	LOGGER.debug("No data returned for Student Perf in Collection");
                	return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                }
                
                collectionKpi.forEach(m -> { 
                	unitId = m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString();
                	LOGGER.debug("The Value of UNITID " + unitId);
                	completedCountMap.forEach(map -> { if ((map.get(AJEntityBaseReports.UNIT_GOORU_OID).toString()).equals(unitId)) {
                		compCount = map.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString();
                	}
                	});

                	CourseKpiArray.add(new JsonObject().put(AJEntityBaseReports.UNIT_GOORU_OID, unitId)
                    		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())
                    		.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, compCount)
                    		.put(AJEntityBaseReports.ATTR_ATTEMPT_STATUS, AJEntityBaseReports.NA)
                    		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                    		.put(AJEntityBaseReports.ATTR_COLLVIEWS, m.get(AJEntityBaseReports.ATTR_COLLVIEWS).toString())
                    		.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, AJEntityBaseReports.NA));                    				                	
                });        		
        	} else {
                LOGGER.error("Could not get Student Course Performance");
                return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }        	
                
        }
        
        //Form the required Json pass it on
        contentBody.put(JsonConstants.USAGE_DATA, CourseKpiArray).put(JsonConstants.USERUID, this.userId);
        resultarray.add(contentBody);
        resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

    	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                ExecutionStatus.SUCCESSFUL);
    	
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
