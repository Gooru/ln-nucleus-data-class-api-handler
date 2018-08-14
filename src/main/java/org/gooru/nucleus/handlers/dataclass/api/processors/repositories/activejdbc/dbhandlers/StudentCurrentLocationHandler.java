package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

/**
 * Created by mukul@gooru
 */
public class StudentCurrentLocationHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(StudentCurrentLocationHandler.class);

	  private final ProcessorContext context;

    public StudentCurrentLocationHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {

    	//No Sanity Check required since, no params are being passed in Request Body

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest())) {
        LOGGER.debug("User ID in the session : {}", context.userIdFromSession());
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
        }
        LOGGER.debug("User is teacher of this class.");
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {

    	JsonObject resultBody = new JsonObject();

    	JsonArray currentLocArray = new JsonArray();

        String classId = context.classId();
        String userId = context.getUserIdFromRequest();
        String courseId = context.request().getString(EventConstants.COURSE_ID);
        

    	List<Map> currentLocMap = null;
    	
    	if (StringUtil.isNullOrEmpty(courseId)) currentLocMap = Base.findAll( AJEntityBaseReports.GET_STUDENT_LOCATION, classId, userId);
    	else currentLocMap = Base.findAll( AJEntityBaseReports.GET_STUDENT_LOCATION_IN_CLASS_AND_COURSE, classId, courseId, userId);

    	if (currentLocMap != null && !currentLocMap.isEmpty()){
    	  currentLocMap.forEach(m -> {
          JsonObject loc = new JsonObject();
          loc.put(AJEntityBaseReports.ATTR_CLASS_ID, m.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
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
          List<Map> collectionStatus = Base.findAll(AJEntityBaseReports.GET_COLLECTION_STATUS, m.get(AJEntityBaseReports.SESSION_ID), collId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);
          if (!collectionStatus.isEmpty()) {
              loc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
              if (Objects.equals(m.get(AJEntityBaseReports.COLLECTION_TYPE), EventConstants.ASSESSMENT)) {
                  Map score = collectionStatus.get(0);
                  loc.put(AJEntityBaseReports.ATTR_SCORE, score.get(AJEntityBaseReports.ATTR_SCORE) == null 
                  ? null : Math.round(Double.valueOf(score.get(AJEntityBaseReports.ATTR_SCORE).toString())));
              }
          } else {
              loc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
          }
          currentLocArray.add(loc);
        });

    	} else {
            LOGGER.info("Current Location Attributes cannot be obtained");
        }

        //Form the required JSon pass it on
        resultBody.put(JsonConstants.CONTENT, currentLocArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

    	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                ExecutionStatus.SUCCESSFUL);

    }


    @Override
    public boolean handlerReadOnly() {
        return true;
    }
}
