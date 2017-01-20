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

public class StudentUnitPerfHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudentUnitPerfHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userUid";
        
    private final ProcessorContext context;
    private AJEntityBaseReports baseReport;
    
    private String collectionType;
    private String userId;
    
    //For stuffing Json
    private String unitId;
    private String LId;
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


    public StudentUnitPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Units");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Units"),
                ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.getUserIdFromRequest() == null
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> creator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
        if (creator.isEmpty()) {
          List<Map> collaborator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_COLLABORATOR, this.context.classId(), this.context.userIdFromSession());
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
    	JsonObject collObj = new JsonObject();
        JsonArray collarray = new JsonArray();
    	JsonArray resultarray = new JsonArray();
    	baseReport = new AJEntityBaseReports();
    	
    	//TODO Confirm if collType is optional. In which case we need not check for null or Empty (and probably send data for both)
    	this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
    	if (StringUtil.isNullOrEmpty(collectionType)) {
            LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in course"),
                ExecutionStatus.FAILED);
        }
    	LOGGER.debug("Collection Type is " + this.collectionType);
        
        this.userId = this.context.request().getString(REQUEST_USERID);
        
        List<String> userIds = new ArrayList<>();        
        List<String> lessonIds = new ArrayList<>();
        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is not in the request to fetch Student Performance in Course. Asseume user is a teacher");
            LazyList<AJEntityBaseReports> userIdforUnit = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_UNITID_FITLERBY_COLLTYPE,
                    context.classId(), context.courseId(), context.unitId(), EventConstants.ASSESSMENT);
            userIdforUnit.forEach(lesson -> userIds.add(lesson.getString(AJEntityBaseReports.GOORUUID)));

        }else{
          userIds.add(this.userId);
        }
        for (String userID : userIds) {
          JsonObject contentBody = new JsonObject();
          JsonArray UnitKpiArray = new JsonArray();
          JsonArray assessmentArray = new JsonArray();
          JsonArray collectionArray = new JsonArray();
        //TODO: Maybe a SWITCH STATEMENT
        if (collectionType.equals(EventConstants.ASSESSMENT) || collectionType.equals(EventConstants.BOTH)) {
        	LazyList<AJEntityBaseReports> lessonIDforUnit = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_LESSONID_FOR_UNITID_FITLERBY_COLLTYPE,
                    context.classId(), context.courseId(), context.unitId(), EventConstants.ASSESSMENT, userID);
        	
        	if (!lessonIDforUnit.isEmpty()) {
        		LOGGER.info("Got a list of Distinct lessonIDs for this Course");
                
            	lessonIDforUnit.forEach(lesson -> lessonIds.add(lesson.getString(AJEntityBaseReports.LESSON_GOORU_OID)));
                
            	// For EACH LESSON get the list of ALL ASSESSMENTS. This needs to be stuffed into the RESULT BODY (Tag SourceList:)
                for (String alID : lessonIds) {            	
                	LOGGER.debug (alID);
                	JsonObject collList = new JsonObject();
                	List<String> collIds = new ArrayList<>();
                    JsonArray LessonKpiArray = new JsonArray();
                    
                	LazyList<AJEntityBaseReports> collIDforlesson = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE,
                            context.classId(), context.courseId(), context.unitId(), alID, EventConstants.ASSESSMENT, userID);
                	
                	if (!collIDforlesson.isEmpty()) {        		
                		LOGGER.info("Got a list of Distinct collectionIDs for this lesson");                	
                    	
                    	collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                                                        
                        List<Map> assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT,
                        		listToPostgresArrayString(collIds), userID);
                        
                        if(!assessmentKpi.isEmpty()){
                            assessmentKpi.forEach(m -> LessonKpiArray.add(new JsonObject().put(AJEntityBaseReports.ATTR_ASSESSMENT_ID, m.get(AJEntityBaseReports.COLLECTION_OID).toString())
                            		.put(AJEntityBaseReports.COLLECTION_TYPE, EventConstants.ASSESSMENT)
                            		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())            		
                            		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                            		.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE).toString())
                            		.put(AJEntityBaseReports.ATTR_ATTEMPTS, m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString())
                            		));
                            
                            List<Map> unitKpiList = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_EACH_UNIT_PERF_FOR_ASSESSMENT,
                            		alID, userID);                        
                            
                            List<Map> completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT_FOREACH_LESSONID,
                            		context.classId(), context.courseId(), context.unitId(), alID,
                            		EventConstants.ASSESSMENT, userID, EventConstants.COLLECTION_PLAY, AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
                                            
                            unitKpiList.forEach(m -> { 
                            	LId = m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString();
                            	LOGGER.debug("The Value of CollectionID " + LId);
                                if(completedCountMap.isEmpty()){
                                	LOGGER.debug("No data returned for completedCount");
                                	compCount = AJEntityBaseReports.NA;                                	
                                } else {
                                	completedCountMap.forEach(map -> { if ((map.get(AJEntityBaseReports.LESSON_GOORU_OID).toString()).equals(LId)) {
                                		compCount = map.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString();
                                	}
                                	});                                	
                                }
                            
                            	collList.put(AJEntityBaseReports.LESSON_GOORU_OID, m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString())                    		
                        		.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, compCount)
                        		.put(AJEntityBaseReports.ATTR_ATTEMPT_STATUS, AJEntityBaseReports.NA)
                        		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())            		
                        		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                        		.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE).toString())
                        		.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, AJEntityBaseReports.NA);                	
                            });                
                            
                            collList.put(JsonConstants.SOURCELIST, LessonKpiArray);
                            assessmentArray.add(collList);
                        } else {
                        	LOGGER.debug("No data returned for Student Perf in Assessment");
                        }                        
                	} else {
                        LOGGER.error("Could not get Student Lesson Performance");
                        //Return an empty resultBody instead of an Error
                        //return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                    } 
                } //END - For EACH LESSON get the list of ALL ASSESSMENTS
        		
        	} else {
                LOGGER.error("Could not get Student Lesson Performance");
                //Return an empty resultBody instead of an Error
                //return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            } 
            
        } //END - GET UNIT PERFORMANCE FOR ALL ASSESSMENTS
    
        if (collectionType.equals(EventConstants.COLLECTION) || collectionType.equals(EventConstants.BOTH)) {

        	LazyList<AJEntityBaseReports> lessonIDforUnit = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_LESSONID_FOR_UNITID_FITLERBY_COLLTYPE,
                    context.classId(), context.courseId(), context.unitId(), EventConstants.COLLECTION, userID);
        	
        	if (!lessonIDforUnit.isEmpty()) {
        		LOGGER.debug("Got a list of Distinct unitIDs for this Course");
                
            	lessonIDforUnit.forEach(lesson -> lessonIds.add(lesson.getString(AJEntityBaseReports.LESSON_GOORU_OID)));
                
            	// For EACH LESSON get the list of ALL ASSESSMENTS. This needs to be stuffed into the RESULT BODY (Tag SourceList:)
                for (String clID : lessonIds) {
                	JsonObject collList = new JsonObject();
                	List<String> collIds = new ArrayList<>();
                    JsonArray LessonKpiArray = new JsonArray();
                    
                	LazyList<AJEntityBaseReports> collIDforlesson = AJEntityBaseReports.findBySQL( AJEntityBaseReports.SELECT_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE,
                            context.classId(), context.courseId(), context.unitId(), clID, EventConstants.COLLECTION, userID);
                	
                	if (!collIDforlesson.isEmpty()) {        		
                		LOGGER.debug("Got a list of Distinct collectionIDs for this lesson");                	
                    	
                    	collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                                                        
                        List<Map> collectionKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION,
                        		listToPostgresArrayString(collIds), userID);
                        
                        if(!collectionKpi.isEmpty()){                        	
                        	collectionKpi.forEach(m -> LessonKpiArray.add(new JsonObject().put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.COLLECTION_OID).toString())
                            		.put(AJEntityBaseReports.COLLECTION_TYPE, EventConstants.COLLECTION)
                            		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())            		
                            		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                            		.put(AJEntityBaseReports.ATTR_COLLVIEWS, m.get(AJEntityBaseReports.ATTR_COLLVIEWS).toString())
                            		));
                            
                            List<Map> unitKpiList = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_EACH_UNIT_PERF_FOR_COLLECTION,
                            		clID, userID);

                            List<Map> completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT_FOREACH_LESSONID,
                            		context.classId(), context.courseId(), context.unitId(), clID,
                            		EventConstants.COLLECTION, userID, EventConstants.COLLECTION_PLAY, AJEntityBaseReports.ATTR_EVENTTYPE_STOP);
                            
                            unitKpiList.forEach(m -> { 
                            	LId = m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString();
                            	LOGGER.debug("The Value of CollectionID " + LId);
                                if(completedCountMap.isEmpty()){
                                	LOGGER.debug("Error in your getCount Query, No data returned");
                                	compCount = AJEntityBaseReports.NA;
                                } else {
                                	completedCountMap.forEach(map -> { if ((map.get(AJEntityBaseReports.LESSON_GOORU_OID).toString()).equals(LId)) {
                                		compCount = map.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString();
                                	}
                                	});                                	
                                }
                                
                            	collList.put(AJEntityBaseReports.LESSON_GOORU_OID, m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString())
                        		.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, compCount)
                        		.put(AJEntityBaseReports.ATTR_ATTEMPT_STATUS, AJEntityBaseReports.NA)
                        		.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString())            		
                        		.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION).toString())
                        		.put(AJEntityBaseReports.ATTR_COLLVIEWS, m.get(AJEntityBaseReports.ATTR_COLLVIEWS).toString())
                        		.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, AJEntityBaseReports.NA);                	
                            });                


                            collList.put(JsonConstants.SOURCELIST, LessonKpiArray);
                            collectionArray.add(collList);
                        } else {
                        	LOGGER.debug("No data returned for Student Perf in Collection");
                        }
                        
                	} else {
                        LOGGER.error("Could not get Student Lesson Performance");
                        //Return an empty resultBody instead of an Error
                        //return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                    } 
                } //END - For EACH LESSON get the list of ALL ASSESSMENTS                                    

        	} else {
                LOGGER.error("Could not get Student Lesson Performance");
                //Return an empty resultBody instead of an Error
                //return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
            } 
            
        } //END - GET UNIT PERFORMANCE FOR ALL COLLECTION
        
        if (collectionType.equals(EventConstants.BOTH)){
            collObj.put(JsonConstants.ASSESSMENT, assessmentArray).put(JsonConstants.COLLECTION, collectionArray);
            collarray.add(collObj);      
            contentBody.put(JsonConstants.USAGE_DATA, collarray).put(JsonConstants.USERUID, userID);
            resultarray.add(contentBody);
        }
        
        if (collectionType.equals(EventConstants.ASSESSMENT)){
        	contentBody.put(JsonConstants.USAGE_DATA, assessmentArray).put(JsonConstants.USERUID, userID);
            resultarray.add(contentBody);
        	
        }
        
        if (collectionType.equals(EventConstants.COLLECTION)){
        	contentBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERUID, userID);
            resultarray.add(contentBody);
        }
    }
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
