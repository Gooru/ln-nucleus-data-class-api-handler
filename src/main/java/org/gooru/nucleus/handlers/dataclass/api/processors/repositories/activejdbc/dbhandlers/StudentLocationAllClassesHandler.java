package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityMilestone;
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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudentLocationAllClassesHandler.class);
  private final ProcessorContext context;
  private static final String REQUEST_USERID = "userId";
  private JsonArray classes;
  private String userId;

  StudentLocationAllClassesHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    userId = this.context.request().getString(REQUEST_USERID);
    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is mandatory for fetching Student Performance in classes");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "User Id Missing. Cannot fetch Student Performance in Classes"),
          ExecutionStatus.FAILED);

    }
    classes = this.context.request().getJsonArray(EventConstants.CLASSES);
    LOGGER.debug("userId : {} - classes:{}", userId, classes);

    if (classes == null || classes.isEmpty()) {
      LOGGER.warn("Classes array is mandatory to fetch Student Location in Classes");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Classes array is Missing. Cannot fetch Student Location in Classes"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    // Only student can get his location for All classes
    if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      LOGGER.debug("validateRequest() FAILED");
      return new ExecutionResult<>(
          MessageResponseFactory.createForbiddenResponse("User validation failed"),
          ExecutionStatus.FAILED);
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    Map<String, JsonObject> classMap = new HashMap<>();
    JsonArray classIds = new JsonArray();
    for (Object cls : classes) {
      JsonObject classObject = (JsonObject) cls;
      if ((classObject.containsKey(EventConstants.CLASS_ID)
          && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.CLASS_ID)))
          && classObject.containsKey(EventConstants.COURSE_ID)
          && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.COURSE_ID))) {
        if (!classMap.containsKey(classObject.getString(EventConstants.CLASS_ID))) {
          classMap.put(classObject.getString(EventConstants.CLASS_ID), classObject);
          LOGGER.debug("Inserted JsonObject {}  for classId key {}", classObject,
              classObject.getString(EventConstants.CLASS_ID));
        } else if (classMap.containsKey(classObject.getString(EventConstants.CLASS_ID))) {
          LOGGER.error(
              "Multipe Courses associated with class {}, ignoring course {} for this class ",
              classObject.getString(EventConstants.CLASS_ID),
              classObject.getString(EventConstants.COURSE_ID));
        }
      } else if (classObject.containsKey(EventConstants.CLASS_ID)
          && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.CLASS_ID))) {
        // This case should not arise. However if it does, since fwCode is associated with Course, &
        // since course is not present,
        // fwCode is also assumed to be absent. Even if fwCode is present, in the absence of course,
        // it should be considered as an
        // Anomaly and will be ignored.
        classIds.add(classObject.getString(EventConstants.CLASS_ID));
      }
    }

    if (classIds.isEmpty() && classMap.isEmpty()) {
      LOGGER.warn("ClassIds are mandatory to fetch Student Location in Classes");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "ClassId is missing. Cannot fetch Student Location in Classes"),
          ExecutionStatus.FAILED);
    }
    JsonObject result = new JsonObject();
    JsonArray locArray = new JsonArray();

    if (classMap.isEmpty()) {
      List<Map> studLocData = Base.findAll(AJEntityBaseReports.GET_STUDENT_LOCATION_ALL_CLASSES,
          userId, listToPostgresArrayString(classIds));
      createLocationResponseForAllClasses(locArray, studLocData, null);
    } else {
      Set<String> keys = classMap.keySet();
      for (String key : keys) {
        LOGGER.debug("The keys in the map are " + key);
        JsonObject classObj = classMap.get(key);
        String classId = classObj.getString(EventConstants.CLASS_ID);
        String courseId = classObj.getString(EventConstants.COURSE_ID);
        String fwCode = classObj.getString(EventConstants.FRAMEWORK_CODE);
        List<Map> studLocData =
            Base.findAll(AJEntityBaseReports.GET_STUDENT_LOCATION_IN_CLASS_AND_COURSE, classId,
                courseId, userId);
        createLocationResponseForAllClasses(locArray, studLocData, fwCode);
      }
    }

    result.put(JsonConstants.USAGE_DATA, locArray).put(JsonConstants.USERID, userId);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  @SuppressWarnings("rawtypes")
  private void createLocationResponseForAllClasses(JsonArray locArray, List<Map> studLocData,
      String fwCode) {
    if (!studLocData.isEmpty()) {
      studLocData.forEach(m -> {
        JsonObject studLoc = new JsonObject();
        studLoc.put(AJEntityBaseReports.ATTR_CLASS_ID,
            m.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
        String courseId = m.get(AJEntityBaseReports.COURSE_GOORU_OID) != null
            ? m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString()
            : null;
        studLoc.put(AJEntityBaseReports.ATTR_COURSE_ID, courseId);
        String unitId = m.get(AJEntityBaseReports.UNIT_GOORU_OID) != null
            ? m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString()
            : null;
        studLoc.put(AJEntityBaseReports.ATTR_UNIT_ID, unitId);
        String lessonId = m.get(AJEntityBaseReports.LESSON_GOORU_OID) != null
            ? m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString()
            : null;
        studLoc.put(AJEntityBaseReports.ATTR_LESSON_ID, lessonId);
        String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
        studLoc.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
        Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collectionId);
        studLoc.put(JsonConstants.COLLECTION_TITLE,
            (collTitle != null ? collTitle.toString() : "NA"));
        String collectionType = m.get(AJEntityBaseReports.COLLECTION_TYPE).toString();
        studLoc.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, collectionType);
        AJEntityBaseReports collectionStatus = AJEntityBaseReports.findFirst(
            "session_id = ?  AND collection_id = ? AND event_name = ? AND event_type = ?",
            m.get(AJEntityBaseReports.SESSION_ID).toString(), collectionId,
            EventConstants.COLLECTION_PLAY, EventConstants.STOP);
        if (collectionStatus != null) {
          studLoc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
          if (EventConstants.ASSESSMENT_TYPES.matcher(m.get(AJEntityBaseReports.COLLECTION_TYPE).toString()).matches()) {
            studLoc.put(AJEntityBaseReports.ATTR_SCORE,
                collectionStatus.get(AJEntityBaseReports.SCORE) == null ? null
                    : Math.round(Double
                        .valueOf(collectionStatus.get(AJEntityBaseReports.SCORE).toString())));
          }
        } else {
          studLoc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
        }
        studLoc.put(AJEntityBaseReports.ATTR_PATH_ID,
            m.get(AJEntityBaseReports.ATTR_PATH_ID) == null ? 0L
                : Long.parseLong(m.get(AJEntityBaseReports.ATTR_PATH_ID).toString()));
        studLoc.put(AJEntityBaseReports.ATTR_PATH_TYPE,
            m.get(AJEntityBaseReports.ATTR_PATH_TYPE) == null ? null
                : m.get(AJEntityBaseReports.ATTR_PATH_TYPE).toString());
        if (fwCode != null && courseId != null && unitId != null && lessonId != null) {
          // Fetch milestoneId
          AJEntityMilestone milestone = AJEntityMilestone.findFirst(
              "course_id = ? AND unit_id = ? AND lesson_id = ? AND fw_code = ?",
              UUID.fromString(courseId), UUID.fromString(unitId), UUID.fromString(lessonId),
              fwCode);

          if (milestone != null) {
            studLoc.put(JsonConstants.MILESTONE_ID, milestone.get(AJEntityMilestone.MILESTONE_ID));
          } else {
            LOGGER.error("Milestone Id cannot be obtained for this course {} and framework {}",
                courseId, fwCode);
          }
        }
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
