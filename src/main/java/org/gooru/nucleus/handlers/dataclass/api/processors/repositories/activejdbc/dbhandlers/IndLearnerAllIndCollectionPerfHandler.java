package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class IndLearnerAllIndCollectionPerfHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllIndCollectionPerfHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private String userId;
	  
	  IndLearnerAllIndCollectionPerfHandler(ProcessorContext context) {
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
	    	    
	    JsonObject result = new JsonObject();
	    JsonArray assessmentKpiArray = new JsonArray();
	    
	    // Form the required Json pass it on
	    result.put(JsonConstants.USAGE_DATA, assessmentKpiArray);
	    
	    if (!StringUtil.isNullOrEmpty(this.userId)) {
	    	result.put(JsonConstants.USERID, this.userId);    	    	
	    } else if (StringUtil.isNullOrEmpty(this.userId)) {    	
	    	result.putNull(JsonConstants.USERID);
	    }
	    
	    result.put("ILCollectionPerf", "Work in Progress");
	    
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
