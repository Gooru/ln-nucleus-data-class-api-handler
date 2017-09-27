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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class IndLearnerAllIndCollectionLocHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllIndCollectionLocHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private static final String REQUEST_LIMIT = "limit";
	  private static final String REQUEST_OFFSET = "offset";

    private Integer limit;
	  private Integer offset;

	  IndLearnerAllIndCollectionLocHandler(ProcessorContext context) {
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
		    //TODO: add offset and limit for pagination

		      if (StringUtil.isNullOrEmpty(userId)) {
		          LOGGER.warn("UserID is mandatory for fetching Learner's Collections");
		          return new ExecutionResult<>(
		                  MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Learner's Collections"),
		                  ExecutionStatus.FAILED);

		        }

		    JsonObject result = new JsonObject();
		    JsonArray locArray = new JsonArray();

		    List<Map> ILAssessments;
		    ILAssessments = Base.findAll(AJEntityBaseReports.GET_DISTINCT_COLLECTION_FOR_INDEPENDENT_LEARNER, userId);

		    if (!ILAssessments.isEmpty()) {
		      ILAssessments.forEach(m -> {
		        JsonObject ILloc = new JsonObject();
	          String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
	          ILloc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
	          ILloc.put(JsonConstants.COLLECTION_TITLE, "NA");
	          String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
	          if (!Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_STATUS, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP).isEmpty()){
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
	    result.put(JsonConstants.USAGE_DATA, locArray).put(JsonConstants.USERID, userId);

	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
	  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }

	  private String listToPostgresArrayString(JsonArray inputArrary) {
	    List<String> input = new ArrayList<>(inputArrary.size());
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
