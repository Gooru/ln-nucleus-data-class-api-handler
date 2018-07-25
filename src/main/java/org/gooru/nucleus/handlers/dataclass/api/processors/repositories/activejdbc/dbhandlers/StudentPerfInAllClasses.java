package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassMember;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
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

public class StudentPerfInAllClasses implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudentPerfInAllClasses.class);

  private final ProcessorContext context;
  private static final String REQUEST_USERID = "userId";

    StudentPerfInAllClasses(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    // No Sanity Check required since, no params are being passed in Request
    // Body
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
      JsonArray classes = this.context.request().getJsonArray(EventConstants.CLASSES);

    LOGGER.debug("userId : {} - classes:{}", userId, classes);

    if (classes == null || classes.isEmpty()) {
      LOGGER.warn("Classes array is mandatory to fetch Student Performance in Classes");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Classes array is Missing. Cannot fetch Student Performance in Classes"),
              ExecutionStatus.FAILED);
    }

    JsonObject classObj = new JsonObject();
    for (Object cls : classes) {
        JsonObject classObject = (JsonObject) cls;
        if ((classObject.containsKey(EventConstants.CLASS_ID) && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.CLASS_ID))) && classObject.containsKey(EventConstants.COURSE_ID) && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.COURSE_ID))) 
            classObj.put(classObject.getString(EventConstants.CLASS_ID), classObject.getString(EventConstants.COURSE_ID));
    }
    if (!classObj.isEmpty()) {
        LOGGER.warn("ClassIds and courseIds are mandatory to fetch Student Performance in Classes");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Both or either of classId or courseId is missing. Cannot fetch Student Performance in Classes"),
                ExecutionStatus.FAILED);
    }
    
    JsonObject result = new JsonObject();
    JsonArray classKpiArray = new JsonArray();    

    // Student All Class Data
    if (!StringUtil.isNullOrEmpty(userId)) {
    	for (String classId : classObj.fieldNames()) {
            String courseId = classObj.getString(classId);
            if (!StringUtil.isNullOrEmpty(courseId)) {
            List<Map> classPerfData = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_ALL_CLASS_DATA, classId, courseId, userId);
    	if (!classPerfData.isEmpty()) {
    	      classPerfData.forEach(classData -> {
    	        JsonObject classKPI = new JsonObject();
    	        classKPI.put(AJEntityBaseReports.ATTR_CLASS_ID, classId);
    	        classKPI.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.valueOf(classData.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString())); 
    	        Object classTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT,
    	                courseId);
    	        classKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, classTotalCount != null ? Integer.valueOf(classTotalCount.toString()) : 0);    	        
    	        List<Map> classScoreCompletion = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_ALL_CLASS_COMPLETION_SCORE,
    	                  classId, courseId, userId);

    	        if (classScoreCompletion != null && !classScoreCompletion.isEmpty()) {
    	          classScoreCompletion.forEach(scoreKPI -> {    	            
    	            classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
    	                    Integer.valueOf(scoreKPI.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
    	            classKPI.put(AJEntityBaseReports.ATTR_SCORE, scoreKPI.get(AJEntityBaseReports.ATTR_SCORE) == null 
    	            		? null : Math.round(Double.valueOf(scoreKPI.get(AJEntityBaseReports.ATTR_SCORE).toString())));
    	          });
    	        } else {
        	        classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);
        	        classKPI.putNull(AJEntityBaseReports.ATTR_SCORE);
    	        }
    	        classKpiArray.add(classKPI);
    	      });
    	    }
            }
    	}
    } else if (StringUtil.isNullOrEmpty(userId)) { // TEACHER All Class Data

        for (String classId : classObj.fieldNames()) {
            String courseId = classObj.getString(classId);
		    if (!StringUtil.isNullOrEmpty(courseId)) {
			JsonObject classKPI = new JsonObject();
			Object activeUsersCountObj = Base.firstCell(AJEntityClassMember.SELECT_ACTIVE_USERS_COUNT, classId);
			//FIXME: It should not be null or 0. If activeUsersCount is null or 0, look at sync job is working..
			int activeUsersCount = activeUsersCountObj == null ? 0 : Integer.valueOf(activeUsersCountObj.toString());
			List<Map> classPerfData = Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENTS_CLASS_DATA, classId, courseId);
	    	if (!classPerfData.isEmpty()) {
	    		classPerfData.forEach(classData -> {
	    			classKPI.put(AJEntityBaseReports.ATTR_CLASS_ID, classId);
        	        classKPI.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.valueOf(classData.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
        	        Object classTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT,
        	            courseId);
        	        classKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, classTotalCount != null 
        	        		? (Integer.valueOf(classTotalCount.toString()) * activeUsersCount) : 0);
        	        //classKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
	    	});
	            List<String> userList = new ArrayList<>();
	      	        LazyList<AJEntityBaseReports> studClass =
		              AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_USERS_IN_CLASS, classId);
		      studClass.forEach(users -> userList.add(users.getString(AJEntityBaseReports.GOORUUID)));

		      List<Map> classPerfList = Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENT_CLASS_COMPLETION_SCORE, classId, courseId,
  					listToPostgresArrayString(userList));

  	    	if (!classPerfList.isEmpty()) {
	    		classPerfList.forEach(scoData -> {
    	            classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
    	                    Integer.valueOf(scoData.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
    	            classKPI.put(AJEntityBaseReports.ATTR_SCORE, scoData.get(AJEntityBaseReports.ATTR_SCORE) == null 
    	            		? null : Math.round(Double.valueOf(scoData.get(AJEntityBaseReports.ATTR_SCORE).toString())));
	    		});
  	    	} else {
    	        classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);
    	        classKPI.putNull(AJEntityBaseReports.ATTR_SCORE);
	        }
  	    	
		}
	    	classKpiArray.add(classKPI);
		    }
		}

    } else {
        LOGGER.info("Could not get Student All Class Performance data");
      }

    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, classKpiArray);

    if (!StringUtil.isNullOrEmpty(userId)) {
    	result.put(JsonConstants.USERID, userId);
    } else if (StringUtil.isNullOrEmpty(userId)) {
    	result.putNull(JsonConstants.USERID);
    }

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
