package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
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
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class IndLearnerAllCoursesLocationHandler implements DBHandler {
	
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllCoursesLocationHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private static final String REQUEST_LIMIT = "limit";
	  private static final String REQUEST_OFFSET = "offset";

	  private String userId;
	  private Integer limit;
	  private Integer offset;

	  IndLearnerAllCoursesLocationHandler(ProcessorContext context) {
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
	    // TODO :: Teacher validation to be added.
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> executeRequest() {

	    this.userId = this.context.request().getString(REQUEST_USERID);
	    //TODO: add offset and limit for pagination    
	    		
	      if (StringUtil.isNullOrEmpty(userId)) {
	          LOGGER.warn("UserID is mandatory for fetching Learner's location in Courses");
	          return new ExecutionResult<>(
	                  MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Learner's location in Courses"),
	                  ExecutionStatus.FAILED);
	    
	        }

	    JsonObject result = new JsonObject();
	    JsonArray locArray = new JsonArray();
	    	    
	    List<String> courseIds = new ArrayList<>();
	    
	    //Include offset and limit for pagination
	    LazyList<AJEntityBaseReports> ILCourses = AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_COURSES_FOR_INDEPENDENT_LEARNER, this.userId);
    	ILCourses.forEach(course -> courseIds.add(course.getString(AJEntityBaseReports.COURSE_GOORU_OID)));	  
    	
    	for (String c : courseIds){
    		LOGGER.info("Course Ids are" + c);
    		
    	}

	    List<Map> ILCoursesloc = null;
	    ILCoursesloc = Base.findAll(AJEntityBaseReports.GET_INDEPENDENT_LEARNER_LOCATION_ALL_COURSES, this.userId, listToPostgresArrayString(courseIds));
	    	    
	    if (!ILCoursesloc.isEmpty()) {
	      ILCoursesloc.forEach(m -> {
	        JsonObject ILloc = new JsonObject();
	      String coId = m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString();
	      ILloc.put(AJEntityBaseReports.ATTR_COURSE_ID, coId);          
          ILloc.put(AJEntityBaseReports.ATTR_UNIT_ID,  m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null);
          ILloc.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null);
          String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
          ILloc.put(JsonConstants.COURSE_TITLE, "NA");
          ILloc.put(JsonConstants.COLLECTION_TITLE, "NA");
          ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
          ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
          String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
          if (!Base.findAll(AJEntityBaseReports.GET_IL_COURSE_COLLECTION_STATUS, coId, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP).isEmpty()){
        	  ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
          } else {
        	  ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
          }

          ILloc.put(JsonConstants.LAST_ACCESSED, m.get(JsonConstants.LAST_ACCESSED).toString());
          locArray.add(ILloc);          
	      });
	    } else {
	      LOGGER.info("Location Attributes for the Independent Learner cannot be obtained");
	    }

	    
	    // Form the required Json pass it on
	    result.put(JsonConstants.USAGE_DATA, locArray).put(JsonConstants.USERID, this.userId);
	    //result.put("CoursesLOC", "Work in Progress");

	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
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
