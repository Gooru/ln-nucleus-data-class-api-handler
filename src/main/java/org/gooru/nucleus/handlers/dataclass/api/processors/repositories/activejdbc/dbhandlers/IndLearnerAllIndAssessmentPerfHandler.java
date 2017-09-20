package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
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

public class IndLearnerAllIndAssessmentPerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllIndAssessmentPerfHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private String userId;

	  IndLearnerAllIndAssessmentPerfHandler(ProcessorContext context) {
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
      String limitS = this.context.request().getString("limit");
      String offsetS = this.context.request().getString("offset");

      if (StringUtil.isNullOrEmpty(this.userId)) {
        // If user id is not present in the path, take user id from session token.
        this.userId = this.context.userIdFromSession();
      }

      JsonObject result = new JsonObject();
      JsonArray assessmentKpiArray = new JsonArray();
      String query = StringUtil.isNullOrEmpty(limitS) ? AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_ATTEMPTS_TIMESPENT
              : AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_ATTEMPTS_TIMESPENT + "LIMIT " + Long.valueOf(limitS);

      List<Map> assessmentTS = Base.findAll(query, this.userId, StringUtil.isNullOrEmpty(offsetS) ? 0L : Long.valueOf(offsetS));
      if (!assessmentTS.isEmpty()) {
        assessmentTS.forEach(assessmentTsKpi -> {
          JsonObject assesmentObject = new JsonObject();
          assesmentObject.put(AJEntityBaseReports.ATTR_COLLECTION_ID, assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
          assesmentObject.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(assessmentTsKpi.get(AJEntityBaseReports.TIME_SPENT).toString()));
          assesmentObject.put(AJEntityBaseReports.ATTR_ATTEMPTS, Long.parseLong(assessmentTsKpi.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()));
          List<Map> assessmentCompletionKpi = Base.findAll(AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_SCORE_COMPLETION, this.userId,
                  assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
          if (!assessmentCompletionKpi.isEmpty()) {
            assessmentCompletionKpi.forEach(courseComplettion -> assesmentObject.put(AJEntityBaseReports.ATTR_SCORE,
                    Math.round(Double.valueOf(courseComplettion.get(AJEntityBaseReports.ATTR_SCORE).toString()))));
          } else {
            assesmentObject.put(AJEntityBaseReports.ATTR_SCORE, 0);
          }
          Object title = Base.firstCell(AJEntityContent.GET_TITLE, assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
          assesmentObject.put(JsonConstants.COLLECTION_TITLE, title);
          assessmentKpiArray.add(assesmentObject);
        });
      } else {
        LOGGER.info("No data returned for independant learner all assessment performance");
      }
      // Form the required Json pass it on
      result.put(JsonConstants.USAGE_DATA, assessmentKpiArray);

      result.put(JsonConstants.USERID, this.userId);

      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
    }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }
}
