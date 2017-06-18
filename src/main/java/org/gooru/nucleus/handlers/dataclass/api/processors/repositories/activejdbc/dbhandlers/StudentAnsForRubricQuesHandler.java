package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
import java.util.List;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudentAnsForRubricQuesHandler implements DBHandler {
	
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentAnsForRubricQuesHandler.class);	  
	  private final ProcessorContext context;
	  	  
	  public StudentAnsForRubricQuesHandler(ProcessorContext context) {
	      this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {
	      if (context.request() == null || context.request().isEmpty()) {
	          LOGGER.warn("Invalid request received to fetch Student Answer for Question");
	          return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Answer for Question"),
	              ExecutionStatus.FAILED);
	      }

	      LOGGER.debug("checkSanity() OK");
	      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> validateRequest() {

		  return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	public ExecutionResult<MessageResponse> executeRequest() {
	  JsonObject resultBody = new JsonObject();
	  JsonArray resultarray = new JsonArray();

	  resultBody.put("Rubrics - Answers" , "WORK IN PROGRESS");

	  return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

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
