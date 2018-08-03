package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 *
 */

public class IndLearnerCourseCurrentLocationHandler implements DBHandler {


	  private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerCourseCurrentLocationHandler.class);

	  private final ProcessorContext context;

    IndLearnerCourseCurrentLocationHandler(ProcessorContext context) {
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

		    JsonObject resultBody = new JsonObject();
	    	JsonArray CurrentLocArray = new JsonArray();
		  String courseId = context.courseId();
          String userId = context.getUserIdFromRequest();

	    	List<Map> CurrentLocMap = Base.findAll( AJEntityBaseReports.GET_IL_LOCATION, courseId, userId);

	    	if (!CurrentLocMap.isEmpty()){
	    	  CurrentLocMap.forEach(m -> {
	          JsonObject loc = new JsonObject();
	          loc.put(AJEntityBaseReports.ATTR_COURSE_ID, m.get(AJEntityBaseReports.COURSE_GOORU_OID) != null ? m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString() :null);
	          loc.put(AJEntityBaseReports.ATTR_UNIT_ID, m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString() : null);
	          loc.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString() : null);
	          String collId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
	          if (Objects.equals(m.get(AJEntityBaseReports.COLLECTION_TYPE), EventConstants.ASSESSMENT)) {
	            loc.put(AJEntityBaseReports.ATTR_ASSESSMENT_ID, collId);
	        	Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
	            loc.put(JsonConstants.ASSESSMENT_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
	          } else {
	            loc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.COLLECTION_OID).toString());
	        	Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
	            loc.put(JsonConstants.COLLECTION_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
	          }
	          loc.put(AJEntityBaseReports.ATTR_PATH_ID, m.get(AJEntityBaseReports.ATTR_PATH_ID) == null ? 0L : Long.parseLong(m.get(AJEntityBaseReports.ATTR_PATH_ID).toString()));
	          loc.put(AJEntityBaseReports.ATTR_PATH_TYPE, m.get(AJEntityBaseReports.ATTR_PATH_TYPE) == null ? null : m.get(AJEntityBaseReports.ATTR_PATH_TYPE).toString());
	          List<Map> collectionStatus = Base.findAll(AJEntityBaseReports.GET_COLLECTION_STATUS, m.get(AJEntityBaseReports.SESSION_ID).toString(), collId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
              if (!collectionStatus.isEmpty()){
                  loc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
                if (Objects.equals(m.get(AJEntityBaseReports.COLLECTION_TYPE), EventConstants.ASSESSMENT)) {
                    Map score = collectionStatus.get(0);
                    loc.put(AJEntityBaseReports.ATTR_SCORE, score.get(AJEntityBaseReports.ATTR_SCORE) == null 
                    ? null : Math.round(Double.valueOf(score.get(AJEntityBaseReports.ATTR_SCORE).toString())));
                }
              } else {
                  loc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
              }
	          CurrentLocArray.add(loc);
	        });

	    	} else {
	            LOGGER.info("Current Location Attributes cannot be obtained for the Independent Learner");
	        }

	        //Form the required JSon pass it on
	        resultBody.put(JsonConstants.CONTENT, CurrentLocArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

	    	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
	                ExecutionStatus.SUCCESSFUL);
	  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }

}
