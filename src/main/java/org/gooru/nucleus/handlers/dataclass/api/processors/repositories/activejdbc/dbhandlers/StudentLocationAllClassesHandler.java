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

/**
 * Created by mukul@gooru
 */
public class StudentLocationAllClassesHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentLocationAllClassesHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";

	StudentLocationAllClassesHandler(ProcessorContext context) {
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
		  JsonArray classIds = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
	    LOGGER.debug("userId : {} - classIds:{}", userId, classIds);

	    if (classIds.isEmpty()) {
	      LOGGER.warn("ClassIds are mandatory to fetch Student Performance in Classes");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("ClassIds is Missing. Cannot fetch Student Performance in Classes"),
	              ExecutionStatus.FAILED);
	    }

	    JsonObject result = new JsonObject();
	    JsonArray locArray = new JsonArray();


	    List<Map> studLocData = null;
	    if (!StringUtil.isNullOrEmpty(userId)) {
	      studLocData = Base.findAll(AJEntityBaseReports.GET_STUDENT_LOCATION_ALL_CLASSES, userId, listToPostgresArrayString(
			  classIds));
	    }

	    // TODO: AM - If we are not returning in above statement when userID is null, we are going to produce NPE
	    if (!studLocData.isEmpty()) {
	      studLocData.forEach(m -> {
	        JsonObject studLoc = new JsonObject();
	      studLoc.put(AJEntityBaseReports.ATTR_CLASS_ID, m.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
          studLoc.put(AJEntityBaseReports.ATTR_COURSE_ID, m.get(AJEntityBaseReports.COURSE_GOORU_OID) != null ? m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString() : null);
          studLoc.put(AJEntityBaseReports.ATTR_UNIT_ID,  m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null);
          studLoc.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null);
          String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
          studLoc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
      	  Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
          studLoc.put(JsonConstants.COLLECTION_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
          studLoc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
          String sessionId = m.get(AJEntityBaseReports.SESSION_ID).toString();
          if (!Base.findAll(AJEntityBaseReports.GET_COLLECTION_STATUS, sessionId, collectionId, EventConstants.COLLECTION_PLAY, EventConstants.STOP).isEmpty()){
        	  studLoc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
          } else {
        	  studLoc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
          }
          locArray.add(studLoc);
	      });
	    } else {
	      LOGGER.info("Location Attributes for the student cannot be obtained");
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
