package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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
 * @author mukul@gooru
 * 
 */
public class StudDCAAllClassPerfHandler implements DBHandler {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudDCAAllClassPerfHandler.class);
	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private JsonArray classIds;

	  StudDCAAllClassPerfHandler(ProcessorContext context) {
	    this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {
	    LOGGER.debug("checkSanity() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  public ExecutionResult<MessageResponse> validateRequest() {
	      if (context.getUserIdFromRequest() == null
	              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
	        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
	        if (owner.isEmpty()) {
	            LOGGER.debug("validateRequest() FAILED");
	            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
	        }
	      }
	        LOGGER.debug("validateRequest() OK");
	        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> executeRequest() {

	    String userId = this.context.request().getString(REQUEST_USERID);
	    //String classId = this.context.classId();
	    this.classIds = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
	    LOGGER.debug("userId : {} - classIds:{}", userId, this.classIds);
	    
	    if (classIds.isEmpty()) {
	      LOGGER.warn("ClassIds are mandatory to fetch Student Performance in Classess");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("ClassIds is Missing. Cannot fetch Student Performance in Classes"),
	              ExecutionStatus.FAILED);
	    }
	    
	    List<String> clsIds = new ArrayList<>();
	    for (Object s : this.classIds) {
	      clsIds.add(s.toString());
	}
  
	    JsonObject result = new JsonObject();
	    JsonArray classKpiArray = new JsonArray();    

	    // Student Class Data
	    if (!StringUtil.isNullOrEmpty(userId)) {
	    	for (String clId : clsIds) {
    	        JsonObject classKPI = new JsonObject();
    	        //Average performance across All attempts of A/Ext-A
    	        List<Map> classScoreCompletion = Base.findAll(AJEntityDailyClassActivity.SELECT_STUDENT_CLASS_COMPLETION_SCORE,
    	                  clId, userId);
    	        classKPI.put(AJEntityDailyClassActivity.ATTR_CLASS_ID, clId);
    	        if (classScoreCompletion != null && !classScoreCompletion.isEmpty()) {
    	          classScoreCompletion.forEach(scoreKPI -> {    	            
    	            classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT,
    	                scoreKPI.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT) == null 
                        ? 0 : Integer.valueOf(scoreKPI.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT).toString()));
    	            classKPI.put(AJEntityDailyClassActivity.ATTR_SCORE, scoreKPI.get(AJEntityDailyClassActivity.ATTR_SCORE) == null 
    	            		? null : Math.round(Double.valueOf(scoreKPI.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
    	          });
    	        } else {
        	        classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT, 0);
        	        classKPI.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
    	        }
    	        classKpiArray.add(classKPI);	    		
	    	}	   
	    	     // TEACHER View: ALL Student Data
	    } else if (StringUtil.isNullOrEmpty(userId)) { 
	    	for (String clId : clsIds) {
	    		JsonObject classKPI = new JsonObject();
	    		List<String> userList = new ArrayList<>();
	    		LazyList<AJEntityDailyClassActivity> studClass =
	    				AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.GET_DISTINCT_USERS_IN_CLASS, clId);
	    		studClass.forEach(users -> userList.add(users.getString(AJEntityDailyClassActivity.GOORUUID)));
    	        //Average performance across All attempts of A/Ext-A for class members at d_c_a table
	    		List<Map> classPerfList = Base.findAll(AJEntityDailyClassActivity.SELECT_ALL_STUDENT_CLASS_COMPLETION_SCORE,
	    				listToPostgresArrayString(userList), clId);
	    		classKPI.put(AJEntityDailyClassActivity.ATTR_CLASS_ID, clId);
	    		if (!classPerfList.isEmpty()) {
	    			classPerfList.forEach(scoData -> {
	    				classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT, scoData.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT) == null 
	    						? 0 : Integer.valueOf(scoData.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT).toString()));
	    				classKPI.put(AJEntityDailyClassActivity.ATTR_SCORE, scoData.get(AJEntityDailyClassActivity.ATTR_SCORE) == null 
	    						? null : Math.round(Double.valueOf(scoData.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
	    			});
	    		} else {
	    			classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT, 0);
	    			classKPI.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
	    		}

	    		classKpiArray.add(classKPI);
	    	}
	    	
	    } else {
	        LOGGER.info("Could not get Class Activity Performance data for Classes");
	      }

	    result.put(JsonConstants.USAGE_DATA, classKpiArray);
	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
	  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }

	  private String listToPostgresArrayString(List<String> input) {
		    int approxSize = ((input.size() + 1) * 36); // Length of UUID is around 36 chars
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