package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

/**
 * Created by mukul@gooru
 */

public class StudentLessonPerfHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentLessonPerfHandler.class);
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

    public StudentLessonPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Lessons");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Lessons"),
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
    	JsonObject contentBody = new JsonObject();
    	JsonArray resultarray = new JsonArray();
    	baseReport = new AJEntityBaseReports();
    	
        //CollectionType is a Mandatory Parameter
    	this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
    	if (StringUtil.isNullOrEmpty(collectionType)) {
            LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in course"),
                ExecutionStatus.FAILED);
        }
    	LOGGER.debug("Collection Type is " + this.collectionType);
        
        this.userId = this.context.request().getString(REQUEST_USERID);
        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is mandatory to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("UserID Missing. Cannot fetch Student Performance in course"),
                ExecutionStatus.FAILED);
        }
        LOGGER.debug("UID is " + this.userId);
        
        List<String> collIds = new ArrayList<>();
        JsonArray LessonKpiArray = new JsonArray();
        
        //If CollectionType is Assessment
        if (collectionType.equals(EventConstants.ASSESSMENT)) {
        	LazyList<AJEntityBaseReports> collIDforlesson = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE,
                    context.classId(), context.courseId(), context.unitId(), context.lessonId(), this.collectionType, this.userId);
        	
        	if (!collIDforlesson.isEmpty()) {        		
        		LOGGER.debug("Got a list of Distinct collectionIDs for this lesson");
                    
            	collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                                
                List<Map> completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT,
                		context.classId(), context.courseId(), context.unitId(), context.lessonId(),
                		this.collectionType, this.userId, EventConstants.COLLECTION_PLAY, AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
                        
                List<Map> assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT,
                		listToPostgresArrayString(collIds), this.userId);
                
                if(!assessmentKpi.isEmpty()){                	
                	assessmentKpi.forEach(m -> { 
                    	collId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
                    	LOGGER.debug("The Value of CollectionID " + collId);
                        if(completedCountMap.isEmpty()){
                        	LOGGER.debug("No data returned for completedCount");
                        	compCount = AJEntityBaseReports.NA;
                        } else {
                        	completedCountMap.forEach(map -> { if ((map.get(AJEntityBaseReports.COLLECTION_OID).toString()).equals(collId)) {
                        		compCount = map.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString();
                        	}
                        	});                    	
                        }
                    	
                    	LessonKpiArray.add(new JsonObject().put(AJEntityBaseReports.COLLECTION_OID, collId)
                        		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())
                        		.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, compCount)
                        		.put(AJEntityBaseReports.ATTR_ATTEMPT_STATUS, AJEntityBaseReports.NA)
                        		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                        		.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE).toString())
                        		.put(AJEntityBaseReports.ATTR_ATTEMPTS, m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString())
                        		.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, AJEntityBaseReports.NA));                	
                    });
                } else {
                    //Return an empty resultBody instead of an Error
                	LOGGER.debug("No data returned for Student Perf in Assessment");                	
                }              
                
        	} else {
                LOGGER.info("Could not get Student Lesson Performance");
                //Return an empty resultBody instead of an Error                
                //return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }
                
        }
    
        //If collection is collection
        if (collectionType.equals(EventConstants.COLLECTION)) {
        	LazyList<AJEntityBaseReports> collIDforlesson = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE,
        			context.classId(), context.courseId(), context.unitId(), context.lessonId(), this.collectionType, this.userId);
        	
        	if (!collIDforlesson.isEmpty()) {        		
        		LOGGER.debug("Got a list of Distinct unitIDs for this Course");
        
            	collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                
                List<Map> completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT,
                		context.classId(), context.courseId(), context.unitId(), context.lessonId(),
                		this.collectionType, this.userId, EventConstants.COLLECTION_PLAY, AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
                                
                List<Map> collectionKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION,
                		listToPostgresArrayString(collIds), this.userId);
                
                if(!collectionKpi.isEmpty()){                	
                	collectionKpi.forEach(m -> { 
                    	collId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
                    	LOGGER.debug("The Value of CollectionID " + collId);
                        if(completedCountMap.isEmpty()){
                        	LOGGER.debug("No data returned for completedCount");
                        	compCount = AJEntityBaseReports.NA;
                        } else {
                        	completedCountMap.forEach(map -> { if ((map.get(AJEntityBaseReports.COLLECTION_OID).toString()).equals(collId)) {
                        		compCount = map.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString();
                        	}
                        	});                	                    	
                        }
                    	LessonKpiArray.add(new JsonObject().put(AJEntityBaseReports.COLLECTION_OID, collId)
                        		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())
                        		.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, compCount)
                        		.put(AJEntityBaseReports.ATTR_ATTEMPT_STATUS, AJEntityBaseReports.NA)
                        		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())                    		
                        		.put(AJEntityBaseReports.ATTR_COLLVIEWS, m.get(AJEntityBaseReports.ATTR_COLLVIEWS).toString())
                        		.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, AJEntityBaseReports.NA));                	
                    });                

                } else {
                	LOGGER.debug("No data returned for Student Perf in Collection");
                }                
        	}  else {
                LOGGER.info("Could not get Student Lesson Performance");
                //Return an empty resultBody instead of an Error                
                //return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            }
        }

        contentBody.put(JsonConstants.USAGE_DATA, LessonKpiArray).put(JsonConstants.USERUID, this.userId);
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
