package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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

public class IndLearnerAllIndAssessmentLocHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllIndAssessmentLocHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";

	  IndLearnerAllIndAssessmentLocHandler(ProcessorContext context) {
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

          String userId = this.context.request().getString(REQUEST_USERID);
		    //TODO: add offset and limit for pagination


		      if (StringUtil.isNullOrEmpty(userId)) {
		          LOGGER.warn("UserID is mandatory for fetching Learner's Assessments");
		          return new ExecutionResult<>(
		                  MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Learner's Assessments"),
		                  ExecutionStatus.FAILED);

		        }

		    JsonObject result = new JsonObject();
		    JsonArray locArray = new JsonArray();

		    List<Map> ILAssessments;
		    ILAssessments = Base.findAll(AJEntityBaseReports.GET_DISTINCT_ASSESSMENT_FOR_INDEPENDENT_LEARNER, userId);

		    if (!ILAssessments.isEmpty()) {
		      ILAssessments.forEach(m -> {
		        JsonObject ILloc = new JsonObject();
	          String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
	          ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
	          ILloc.put(JsonConstants.COLLECTION_TITLE, "NA");
	          String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
	          List<Map> completedAssessment = Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_STATUS, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
	          if (!completedAssessment.isEmpty()) {
	              ILloc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
	              Map score = completedAssessment.get(0);
	              ILloc.put(AJEntityBaseReports.ATTR_SCORE, score.get(AJEntityBaseReports.ATTR_SCORE) == null 
                      ? null : Math.round(Double.valueOf(score.get(AJEntityBaseReports.ATTR_SCORE).toString())));
	          } else {
	        	  ILloc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
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

}
