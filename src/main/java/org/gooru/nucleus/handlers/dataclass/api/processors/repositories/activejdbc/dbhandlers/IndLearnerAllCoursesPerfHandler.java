package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
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
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    this.userId = this.context.request().getString(REQUEST_USERID);
    String limitS = this.context.request().getString("limit");
    String offsetS = this.context.request().getString("offset");

    if (StringUtil.isNullOrEmpty(this.userId)) {
      // If user id is not present in the path, take user id from session token.
      this.userId = this.context.userIdFromSession();
    }

    JsonObject result = new JsonObject();
    JsonArray courseKpiArray = new JsonArray();
    String query = StringUtil.isNullOrEmpty(limitS) ? AJEntityBaseReports.GET_IL_ALL_COURSE_TIMESPENT
            : AJEntityBaseReports.GET_IL_ALL_COURSE_TIMESPENT + "LIMIT " + Long.valueOf(limitS);

    List<Map> courseTSKpi = Base.findAll(query, this.userId, StringUtil.isNullOrEmpty(offsetS) ? 0L : Long.valueOf(offsetS));
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
        Object title = Base.firstCell(AJEntityContent.GET_TITLE, courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
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
	}

