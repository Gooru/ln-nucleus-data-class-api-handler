package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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

public class QuestionsToGradeHandler implements DBHandler {
	
  private static final Logger LOGGER = LoggerFactory.getLogger(QuestionsToGradeHandler.class);
  private final ProcessorContext context;
    
  public QuestionsToGradeHandler(ProcessorContext context) {
      this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
      if (context.request() == null || context.request().isEmpty()) {
          LOGGER.warn("Invalid request recieved to fetch Questions to grade");
          return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Questions to grade"),
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

  resultBody.put("Rubrics - QuestionsToGrade" , "WORK IN PROGRESS");

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
