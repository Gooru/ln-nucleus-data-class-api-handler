package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityILBookmarkContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityMilestone;
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

public class IndependentLearnerLocationHandler implements DBHandler {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(IndependentLearnerLocationHandler.class);
	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private static final String REQUEST_CONTENTTYPE = "contentType";
	  private static final int MAX_LIMIT = 20;
	  private String userId;
	  
	  IndependentLearnerLocationHandler(ProcessorContext context) {
	    this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {	
		    userId = this.context.request().getString(REQUEST_USERID);
		    if (StringUtil.isNullOrEmpty(userId)) {
		        userId = this.context.request().getString(REQUEST_USERID);
		        if (StringUtil.isNullOrEmpty(userId)) {
		            LOGGER.error("userId is Mandatory to fetch Independent Learner's performance");
		            return new ExecutionResult<>(
		                    MessageResponseFactory.createInvalidRequestResponse("userId Missing. Cannot fetch Independent Learner's Performance"),
		                    ExecutionStatus.FAILED);
		          }
		      }
	    LOGGER.debug("checkSanity() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  public ExecutionResult<MessageResponse> validateRequest() {
	      if (context.getUserIdFromRequest() == null
	              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
	          LOGGER.debug("validateRequest() FAILED");
	          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User validation failed"), ExecutionStatus.FAILED);	        
	      }

	    LOGGER.debug("validateRequest() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> executeRequest() {
 	    String limitS = this.context.request().getString("limit");	    
	    String offsetS = this.context.request().getString("offset");
	    String fwCode = context.request().getString(EventConstants.FRAMEWORK_CODE);
	           		
	    JsonObject result = new JsonObject();
	    JsonArray locArray = new JsonArray();
	    List<Map> ILlocMap = null;
	    
	    String contentType = this.context.request().getString(REQUEST_CONTENTTYPE);
	  
	    if (!StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.COURSE)){
	    	
			//@NILE-1329
	    	List<String> courseIds = new ArrayList<>();
	    	LazyList<AJEntityILBookmarkContent> ILCourses = AJEntityILBookmarkContent.findBySQL(AJEntityILBookmarkContent.SELECT_DISTINCT_IL_CONTENTID, userId, AJEntityILBookmarkContent.ATTR_COURSE);
		  	if (!ILCourses.isEmpty()) {
		  		
		  		ILCourses.forEach(course -> courseIds.add(course.get(AJEntityILBookmarkContent.CONTENT_ID).toString()));
			  	ILlocMap = Base.findAll(AJEntityBaseReports.GET_INDEPENDENT_LEARNER_LOCATION_ALL_COURSES, 
			  			userId, listToPostgresArrayString(courseIds),
			  			(StringUtil.isNullOrEmpty(limitS) || (Integer.valueOf(limitS) > MAX_LIMIT)) ? MAX_LIMIT : Integer.valueOf(limitS),
			  					StringUtil.isNullOrEmpty(offsetS) ? 0 : Integer.valueOf(offsetS));
		  	}
	    //contentType will be assessment for both assessment|external-assessment	
	    } else if (!StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {
			
	    	//@NILE-1329
	    	List<String> assessmentIds = new ArrayList<>();
	    	LazyList<AJEntityILBookmarkContent> ILAssessments = AJEntityILBookmarkContent.findBySQL(AJEntityILBookmarkContent.SELECT_DISTINCT_IL_ASSESSMENT_ID,
	    			userId);
	    	if (!ILAssessments.isEmpty()) {
	    		ILAssessments.forEach(a -> assessmentIds.add(a.get(AJEntityILBookmarkContent.CONTENT_ID).toString()));
		    	ILlocMap = Base.findAll(AJEntityBaseReports.GET_DISTINCT_ASSESSMENT_FOR_INDEPENDENT_LEARNER, listToPostgresArrayString(assessmentIds), userId, 
			  			(StringUtil.isNullOrEmpty(limitS) || (Integer.valueOf(limitS) > MAX_LIMIT)) ? MAX_LIMIT : Integer.valueOf(limitS),
			  					StringUtil.isNullOrEmpty(offsetS) ? 0 : Integer.valueOf(offsetS));
	    	}
		    //contentType will be collection for both collection|external-collection
	    } else if (!StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.COLLECTION)) {
	    	
			//@NILE-1329
	    	List<String> collectionIds = new ArrayList<>();
	    	LazyList<AJEntityILBookmarkContent> ILCollections = AJEntityILBookmarkContent.findBySQL(AJEntityILBookmarkContent.SELECT_DISTINCT_IL_COLLECTION_ID, 
	    			userId);
	    	if (!ILCollections.isEmpty()) {
	    		ILCollections.forEach(a -> collectionIds.add(a.get(AJEntityILBookmarkContent.CONTENT_ID).toString()));    	  	
	    	  	ILlocMap = Base.findAll(AJEntityBaseReports.GET_DISTINCT_COLLECTION_FOR_INDEPENDENT_LEARNER, listToPostgresArrayString(collectionIds),userId,
			  			(StringUtil.isNullOrEmpty(limitS) || (Integer.valueOf(limitS) > MAX_LIMIT)) ? MAX_LIMIT : Integer.valueOf(limitS),
			  					StringUtil.isNullOrEmpty(offsetS) ? 0 : Integer.valueOf(offsetS));
	    	}	    	 	    	
	    }
	    	    
	    if ((ILlocMap != null) && (!ILlocMap.isEmpty())) {
	    ILlocMap.forEach(m -> {
	    JsonObject ILloc = new JsonObject();
	    String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
        String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
        String collectionType = m.get(AJEntityBaseReports.COLLECTION_TYPE).toString();
        if (contentType.equalsIgnoreCase(MessageConstants.COURSE)) {
        	String coId = m.get(AJEntityBaseReports.COURSE_GOORU_OID) != null ? m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString() : null;	    
    	    ILloc.put(AJEntityBaseReports.ATTR_COURSE_ID, coId); 
    	    String unitId = m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null;
            ILloc.put(AJEntityBaseReports.ATTR_UNIT_ID, unitId);
            String lessonId = m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null;
            ILloc.put(AJEntityBaseReports.ATTR_LESSON_ID, lessonId);
        	Object title = Base.firstCell(AJEntityContent.GET_TITLE, coId);
        	ILloc.put(JsonConstants.COURSE_TITLE, (title != null ? title.toString() : "NA"));
        	Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
            ILloc.put(JsonConstants.COLLECTION_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
            ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
        	ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, collectionType);
        	List<Map> completedCourseCollection = Base.findAll(AJEntityBaseReports.GET_IL_COURSE_COLLECTION_STATUS, 
        			coId, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
        	if (!completedCourseCollection.isEmpty()){
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            	  if (collectionType.equalsIgnoreCase(MessageConstants.ASSESSMENT) 
            			  || collectionType.equalsIgnoreCase(MessageConstants.EXT_ASSESSMENT)) {
            	      Map score = completedCourseCollection.get(0);
            	      ILloc.put(AJEntityBaseReports.ATTR_SCORE, score.get(AJEntityBaseReports.ATTR_SCORE) == null 
                      ? null : Math.round(Double.valueOf(score.get(AJEntityBaseReports.ATTR_SCORE).toString())));
            	  }
            } else {
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
            }     
            if (fwCode != null && coId != null && unitId != null && lessonId != null) {
            	//Fetch milestoneId            	
            	AJEntityMilestone milestone = AJEntityMilestone.findFirst("course_id = ? AND unit_id = ? AND lesson_id = ? AND fw_code = ?", UUID.fromString(coId),
            			UUID.fromString(unitId), UUID.fromString(lessonId), fwCode);
            	if (milestone != null) {
            		ILloc.put(JsonConstants.MILESTONE_ID, milestone.get(AJEntityMilestone.MILESTONE_ID));
            	} else {
            		LOGGER.error("Milestone Id cannot be obtained for this course {} and framework {}", coId, fwCode);
            	}
            }
        } else if (MessageConstants.COLLECTION_TYPES.matcher(contentType).matches()) {
            List<Map> completedCollection = Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_STATUS, 
            		sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
            if (contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {        
                Object title = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
                ILloc.put(JsonConstants.COLLECTION_TITLE, (title != null ? title.toString() : "NA"));
                ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
                ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, collectionType);
            } else if (contentType.equalsIgnoreCase(MessageConstants.COLLECTION)) {
                Object title = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
                ILloc.put(JsonConstants.COLLECTION_TITLE, (title != null ? title.toString() : "NA"));
                ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
                ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, collectionType);
            }
            if (!completedCollection.isEmpty()) {
                ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
                Map score = completedCollection.get(0);
                if (contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {
                	ILloc.put(AJEntityBaseReports.ATTR_SCORE, score.get(AJEntityBaseReports.ATTR_SCORE) == null 
                            ? null : Math.round(Double.valueOf(score.get(AJEntityBaseReports.ATTR_SCORE).toString())));                	
                }
            } else {
                ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
            } 
        }
        ILloc.put(JsonConstants.LAST_ACCESSED, m.get(JsonConstants.LAST_ACCESSED).toString());
        ILloc.put(AJEntityBaseReports.ATTR_PATH_ID, m.get(AJEntityBaseReports.ATTR_PATH_ID) == null ? 0L : Long.parseLong(m.get(AJEntityBaseReports.ATTR_PATH_ID).toString()));
        ILloc.put(AJEntityBaseReports.ATTR_PATH_TYPE, m.get(AJEntityBaseReports.ATTR_PATH_TYPE) == null ? null : m.get(AJEntityBaseReports.ATTR_PATH_TYPE).toString());
        locArray.add(ILloc);          
	      });
	    } else {
	      LOGGER.info("Location Attributes for the Independent Learner cannot be obtained");
	    }
	    
	    // Form the required Json pass it on
	    result.put(JsonConstants.USAGE_DATA, locArray).put(JsonConstants.USERID, userId);

	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
	  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }

	  private String listToPostgresArrayString(List<String> input) {
		  int approxSize = ((input.size() + 1) * 36); 
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

