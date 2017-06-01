package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
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
	  
	  IndependentLearnerLocationHandler(ProcessorContext context) {
	    this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {
	    // No Sanity Check required at this point since, no params are being passed in Request	    
	    LOGGER.debug("checkSanity() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  public ExecutionResult<MessageResponse> validateRequest() {
	    LOGGER.debug("validateRequest() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> executeRequest() {

	    String userId = this.context.request().getString(REQUEST_USERID);
	    if (StringUtil.isNullOrEmpty(userId)) {
	        // If user id is not present in the path, take user id from session token.
	        userId = this.context.userIdFromSession();
	      }
	    String limitS = this.context.request().getString("limit");
	    String offsetS = this.context.request().getString("offset");
       		
	    JsonObject result = new JsonObject();
	    JsonArray locArray = new JsonArray();	    	    
	    List<String> courseIds = new ArrayList<>();
	    List<Map> ILlocMap = null;
	    
	    String contentType = this.context.request().getString(REQUEST_CONTENTTYPE);
	  
	    if (StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.COURSE)){
	       
			LazyList<AJEntityBaseReports> ILCourses = AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_COURSES_FOR_INDEPENDENT_LEARNER, userId);
		  	ILCourses.forEach(course -> courseIds.add(course.getString(AJEntityBaseReports.COURSE_GOORU_OID)));	  
		  	
		  	for (String c : courseIds){
		  		LOGGER.info("Course Ids are" + c);	  		
		  	}

		    ILlocMap = Base.findAll(AJEntityBaseReports.GET_INDEPENDENT_LEARNER_LOCATION_ALL_COURSES, userId, listToPostgresArrayString(courseIds));
	    	
	    } else if (StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {
	    	ILlocMap = Base.findAll(AJEntityBaseReports.GET_DISTINCT_ASSESSMENT_FOR_INDEPENDENT_LEARNER, userId);
	    	
	    } else if (StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.COLLECTION)) {
	    	 ILlocMap = Base.findAll(AJEntityBaseReports.GET_DISTINCT_COLLECTION_FOR_INDEPENDENT_LEARNER, userId);
	    	
	    }
	    
	    	    
	    if (!ILlocMap.isEmpty()) {
	    ILlocMap.forEach(m -> {
	    JsonObject ILloc = new JsonObject();
	    String coId = m.get(AJEntityBaseReports.COURSE_GOORU_OID) != null ? m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString() : null;	    
	    ILloc.put(AJEntityBaseReports.ATTR_COURSE_ID, coId);          
        ILloc.put(AJEntityBaseReports.ATTR_UNIT_ID,  m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null);
        ILloc.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null);
        String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
        String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();        
        if (!StringUtil.isNullOrEmpty(coId) && contentType.equalsIgnoreCase(MessageConstants.COURSE)) {
        	Object title = Base.firstCell(AJEntityContent.GET_TITLE, coId);
        	ILloc.put(JsonConstants.COURSE_TITLE, (title != null ? title.toString() : "NA"));
        	Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
            ILloc.put(JsonConstants.COLLECTION_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
            ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
        	ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
        	if (!Base.findAll(AJEntityBaseReports.GET_IL_COURSE_COLLECTION_STATUS, coId, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP).isEmpty()){
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
              } else {
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
              }        	
        }
        
        if (StringUtil.isNullOrEmpty(coId) && contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {        	
        	ILloc.put(JsonConstants.COURSE_TITLE, "NA");
        	Object title = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
            ILloc.put(JsonConstants.COLLECTION_TITLE, (title != null ? title.toString() : "NA"));
            ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
        	ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
        	if (!Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_STATUS, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP).isEmpty()){
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
              } else {
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
              }        	
        }
        
        if (StringUtil.isNullOrEmpty(coId) && contentType.equalsIgnoreCase(MessageConstants.COLLECTION)) {        	
        	ILloc.put(JsonConstants.COURSE_TITLE, "NA");
        	Object title = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
            ILloc.put(JsonConstants.COLLECTION_TITLE, (title != null ? title.toString() : "NA"));
            ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
        	ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
        	if (!Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_STATUS, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP).isEmpty()){
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
              } else {
            	  ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
              }        	
        }        

        ILloc.put(JsonConstants.LAST_ACCESSED, m.get(JsonConstants.LAST_ACCESSED).toString());
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

