package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.HashMap;
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
          if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is mandatory for fetching Student Performance in classes");
            return new ExecutionResult<>(
                    MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Classes"),
                    ExecutionStatus.FAILED);

          }
          JsonArray classes = this.context.request().getJsonArray(EventConstants.CLASSES);
          LOGGER.debug("userId : {} - classes:{}", userId, classes);

          if (classes == null || classes.isEmpty()) {
            LOGGER.warn("Classes array is mandatory to fetch Student Location in Classes");
            return new ExecutionResult<>(
                    MessageResponseFactory.createInvalidRequestResponse("Classes array is Missing. Cannot fetch Student Location in Classes"),
                    ExecutionStatus.FAILED);
          }

          List<Map<String, String>> classList = new ArrayList<>(classes.size());
          JsonArray classIds = new JsonArray();
          for (Object cls : classes) {
              JsonObject classObject = (JsonObject) cls;
              if ((classObject.containsKey(EventConstants.CLASS_ID) && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.CLASS_ID))) && classObject.containsKey(EventConstants.COURSE_ID) && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.COURSE_ID))) {
                  Map<String, String> classObj = new HashMap<>(2);
                  classObj.put(EventConstants.CLASS_ID, classObject.getString(EventConstants.CLASS_ID));
                  classObj.put(EventConstants.COURSE_ID, classObject.getString(EventConstants.COURSE_ID));
                  classList.add(classObj);
              } else if (classObject.containsKey(EventConstants.CLASS_ID) && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.CLASS_ID))) {
                  classIds.add(classObject.getString(EventConstants.CLASS_ID));
              }
          }
          
          if (classIds.isEmpty() && classList.isEmpty()) {
              LOGGER.warn("ClassIds are mandatory to fetch Student Location in Classes");
              return new ExecutionResult<>(
                      MessageResponseFactory.createInvalidRequestResponse("ClassId is missing. Cannot fetch Student Location in Classes"),
                      ExecutionStatus.FAILED);
          }
	    JsonObject result = new JsonObject();
	    JsonArray locArray = new JsonArray();

        if (classList.isEmpty()) {
            List<Map> studLocData = Base.findAll(AJEntityBaseReports.GET_STUDENT_LOCATION_ALL_CLASSES, userId,
                listToPostgresArrayString(classIds));
            createLocationResponseForAllClasses(locArray, studLocData);
        } else {
            classList.forEach(classObj -> {
                String classId = classObj.get(EventConstants.CLASS_ID);
                String courseId = classObj.get(EventConstants.COURSE_ID);
                List<Map> studLocData = Base.findAll(AJEntityBaseReports.GET_STUDENT_LOCATION_IN_CLASS_AND_COURSE,
                    classId, courseId, userId);
                createLocationResponseForAllClasses(locArray, studLocData);
            });
        }

	    // Form the required Json pass it on
	    result.put(JsonConstants.USAGE_DATA, locArray).put(JsonConstants.USERID, userId);

	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
	  }

    @SuppressWarnings("rawtypes")
    private void createLocationResponseForAllClasses(JsonArray locArray, List<Map> studLocData) {
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
                studLoc.put(AJEntityBaseReports.ATTR_PATH_ID, m.get(AJEntityBaseReports.ATTR_PATH_ID) == null ? 0L : Long.parseLong(m.get(AJEntityBaseReports.ATTR_PATH_ID).toString()));
                studLoc.put(AJEntityBaseReports.ATTR_PATH_TYPE, m.get(AJEntityBaseReports.ATTR_PATH_TYPE) == null ? null : m.get(AJEntityBaseReports.ATTR_PATH_TYPE).toString());
                locArray.add(studLoc);
            });
          } else {
            LOGGER.info("Location Attributes for the student cannot be obtained");
          }
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
