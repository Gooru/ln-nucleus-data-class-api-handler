
package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 *
 */

public class IndLearnerAllCoursesPerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllCoursesPerfHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private String userId;

	IndLearnerAllCoursesPerfHandler(ProcessorContext context) {
	    this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {
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

	this.userId = this.context.request().getString(REQUEST_USERID);
    if (StringUtil.isNullOrEmpty(this.userId)) {
        LOGGER.warn("UserId is mandatory to fetch IL Performance in All Courses");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("UserId Missing. Cannot fetch IL performance in all courses"),
                ExecutionStatus.FAILED);
      }

		  JsonArray courseIds = this.context.request().getJsonArray(MessageConstants.COURSE_IDS);
    LOGGER.debug("userId : {} - courseIds:{}", userId, courseIds);

    if (courseIds.isEmpty()) {
      LOGGER.warn("CourseIds are mandatory to fetch Independent Learner's Performance in Courses");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("CourseIds is Missing. Cannot fetch IL Performance in Course"),
              ExecutionStatus.FAILED);
    }

    List<String> clsIds = new ArrayList<>(courseIds.size());
    for (Object s : courseIds) {
      clsIds.add(s.toString());
    }

    JsonObject result = new JsonObject();
    JsonArray CourseKpiArray = new JsonArray();

    // Getting timespent and attempts.
    List<Map> coursePerfData;

    // Student All Class Data
    	coursePerfData = Base.findAll(AJEntityBaseReports.SELECT_IL_ALL_COURSE_DATA, jArrayToPostgresArrayString(
			courseIds), this.userId);

    	if (!coursePerfData.isEmpty()) {
    	      coursePerfData.forEach(courseData -> {
    	        if(courseData.get(AJEntityBaseReports.COURSE_GOORU_OID) != null){
    	        JsonObject courseKPI = new JsonObject();
    	        courseKPI.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.valueOf(courseData.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
    	        Object courseTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT,
    	                courseData.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());

    	        courseKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, courseTotalCount != null ? Integer.valueOf(courseTotalCount.toString()) : 0);
    	        List<Map> cScoreCompletion = null;
    	          cScoreCompletion = Base.findAll(AJEntityBaseReports.SELECT_IL_ALL_COURSE_COMPLETION_SCORE,
    	                  courseData.get(AJEntityBaseReports.COURSE_GOORU_OID).toString(), this.userId);
    	        if (!cScoreCompletion.isEmpty()) {
    	          cScoreCompletion.forEach(scoreKPI -> {
    	            LOGGER.debug("completedCount : {} ", scoreKPI.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT));
    	            LOGGER.debug("score : {} ", scoreKPI.get(AJEntityBaseReports.ATTR_SCORE));
    	            courseKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
    	                    Integer.valueOf(scoreKPI.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
    	            courseKPI.put(AJEntityBaseReports.ATTR_SCORE, scoreKPI.get(AJEntityBaseReports.ATTR_SCORE) == null ? 0 : Math.round(Double.valueOf(scoreKPI.get(AJEntityBaseReports.ATTR_SCORE).toString())));
    	          });
    	        } else {
        	        courseKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);
        	        courseKPI.putNull(AJEntityBaseReports.ATTR_SCORE);
    	        }
    	        courseKPI.put(AJEntityBaseReports.ATTR_COURSE_ID, courseData.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
    	        CourseKpiArray.add(courseKPI);
    	      }
    	      });
    	    } else {
        LOGGER.info("Could not get Independent Learner Course Performance data");
      }

    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, CourseKpiArray);

    if (!StringUtil.isNullOrEmpty(this.userId)) {
    	result.put(JsonConstants.USERID, this.userId);
    } else if (StringUtil.isNullOrEmpty(this.userId)) {
    	result.putNull(JsonConstants.USERID);
    }

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }

	  private String jArrayToPostgresArrayString(JsonArray inputArrary) {
		    List<String> input = new ArrayList<>();
		    for (Object s : inputArrary) {
		      input.add(s.toString());
		    }
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


