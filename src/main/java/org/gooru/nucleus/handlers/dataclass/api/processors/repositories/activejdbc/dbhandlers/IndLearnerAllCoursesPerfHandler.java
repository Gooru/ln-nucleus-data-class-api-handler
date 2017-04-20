package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
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
	    // No Sanity Check required since, no params are being passed in Request
	    // Body
	    LOGGER.debug("checkSanity() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  public ExecutionResult<MessageResponse> validateRequest() {
	    LOGGER.debug("validateRequest() OK");
	    // FIXME :: Teacher validation to be added.
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
  
      this.userId = this.context.request().getString(REQUEST_USERID);
  
      if (StringUtil.isNullOrEmpty(this.userId)) {
        // If user id is not present in the path, take user id from session token.
        this.userId = this.context.userIdFromSession();
      }
  
      JsonObject result = new JsonObject();
      JsonArray courseKpiArray = new JsonArray();
  
      List<Map> courseTSKpi = Base.findAll(AJEntityBaseReports.GET_IL_ALL_COURSE_TIMESPENT, this.userId);
      if (!courseTSKpi.isEmpty()) {
        courseTSKpi.forEach(courseTS -> {
          JsonObject courseDataObject = new JsonObject();
          courseDataObject.put(AJEntityBaseReports.ATTR_COURSE_ID, courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
          courseDataObject.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(courseTS.get(AJEntityBaseReports.TIME_SPENT).toString()));
          List<Map> courseCompletionKpi = Base.findAll(AJEntityBaseReports.GET_IL_ALL_COURSE_SCORE_COMPLETION, this.userId,
                  courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
          courseCompletionKpi.forEach(courseComplettion -> {
            courseDataObject.put(AJEntityBaseReports.ATTR_SCORE,
                    Math.round(Double.valueOf(courseComplettion.get(AJEntityBaseReports.ATTR_SCORE).toString())));
            courseDataObject.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                    Integer.parseInt(courseComplettion.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
          });
          Object title = Base.firstCell(AJEntityContent.SELECT_COURSE_TITLE, courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
          courseDataObject.put(JsonConstants.COURSE_TITLE, title);
          Object courseTotalAssCount = Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT,
                  courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
          courseDataObject.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, courseTotalAssCount != null ? Integer.valueOf(courseTotalAssCount.toString()) : 0);
          courseKpiArray.add(courseDataObject);
        });
      } else {
        LOGGER.info("NO data returned for indLearner all courses performance");
      }
      // Form the required Json pass it on
      result.put(JsonConstants.USAGE_DATA, courseKpiArray);
  
      result.put(JsonConstants.USERID, this.userId);
  
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
    }

	  @Override
	  public boolean handlerReadOnly() {
	    return false;
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

