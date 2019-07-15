package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityMilestone;
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
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest())) {
      LOGGER.debug("User ID in the session : {}", context.userIdFromSession());
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          this.context.classId(), this.context.userIdFromSession());
      if (owner.isEmpty()) {
        LOGGER.debug("validateRequest() FAILED");
        return new ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
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
    String fwCode = context.request().getString(EventConstants.FRAMEWORK_CODE);
    AJEntityBaseReports locModel = null;

    if (StringUtil.isNullOrEmpty(courseId)) {
      locModel = AJEntityBaseReports.findFirst(
          "class_id = ? AND actor_id = ? AND collection_type IS NOT NULL ORDER BY updated_at DESC",
          classId, userId);
    } else {
      locModel = AJEntityBaseReports.findFirst(
          "class_id = ? AND course_id = ? AND actor_id = ? AND collection_type IS NOT NULL ORDER BY updated_at DESC",
          classId, courseId, userId);
    }

    if (locModel != null) {
      JsonObject loc = new JsonObject();
      loc.put(AJEntityBaseReports.ATTR_CLASS_ID,
          locModel.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
      loc.put(AJEntityBaseReports.ATTR_COURSE_ID,
          locModel.get(AJEntityBaseReports.COURSE_GOORU_OID) != null
              ? locModel.get(AJEntityBaseReports.COURSE_GOORU_OID).toString()
              : null);
      String unitId = locModel.get(AJEntityBaseReports.UNIT_GOORU_OID) != null
          ? locModel.get(AJEntityBaseReports.UNIT_GOORU_OID).toString()
          : null;
      loc.put(AJEntityBaseReports.ATTR_UNIT_ID, unitId);
      String lessonId = locModel.get(AJEntityBaseReports.LESSON_GOORU_OID) != null
          ? locModel.get(AJEntityBaseReports.LESSON_GOORU_OID).toString()
          : null;
      loc.put(AJEntityBaseReports.ATTR_LESSON_ID, lessonId);
      String collId = locModel.get(AJEntityBaseReports.COLLECTION_OID).toString();
      if (EventConstants.ASSESSMENT_TYPES.matcher(locModel.get(AJEntityBaseReports.COLLECTION_TYPE).toString()).matches()) {
        loc.put(AJEntityBaseReports.ATTR_ASSESSMENT_ID, collId);
        Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
        loc.put(JsonConstants.ASSESSMENT_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
      } else {
        loc.put(AJEntityBaseReports.ATTR_COLLECTION_ID,
            locModel.get(AJEntityBaseReports.COLLECTION_OID).toString());
        Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
        loc.put(JsonConstants.COLLECTION_TITLE, (collTitle != null ? collTitle.toString() : "NA"));
      }
      loc.put(AJEntityBaseReports.COLLECTION_TYPE,
          locModel.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
      loc.put(AJEntityBaseReports.ATTR_PATH_ID,
          locModel.get(AJEntityBaseReports.PATH_ID) == null ? 0L
              : Long.parseLong(locModel.get(AJEntityBaseReports.PATH_ID).toString()));
      loc.put(AJEntityBaseReports.ATTR_PATH_TYPE,
          locModel.get(AJEntityBaseReports.PATH_TYPE) == null ? null
              : locModel.get(AJEntityBaseReports.PATH_TYPE).toString());
      AJEntityBaseReports collectionStatus = AJEntityBaseReports.findFirst(
          "session_id = ?  AND collection_id = ? AND event_name = ? AND event_type = ?",
          locModel.get(AJEntityBaseReports.SESSION_ID).toString(), collId,
          EventConstants.COLLECTION_PLAY, EventConstants.STOP);
      if (collectionStatus != null) {
        loc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
        if (EventConstants.ASSESSMENT_TYPES.matcher(locModel.get(AJEntityBaseReports.COLLECTION_TYPE).toString()).matches()) {
          loc.put(AJEntityBaseReports.ATTR_SCORE,
              collectionStatus.get(AJEntityBaseReports.SCORE) == null ? null
                  : Math.round(
                      Double.valueOf(collectionStatus.get(AJEntityBaseReports.SCORE).toString())));
        }
      } else {
        loc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
      }
      // If courseId is not present in the request we will not send milestone details
      if (fwCode != null && courseId != null && unitId != null && lessonId != null) {
        // Fetch milestoneId
        AJEntityMilestone milestone = AJEntityMilestone.findFirst(
            "course_id = ? AND unit_id = ? AND lesson_id = ? AND fw_code = ?",
            UUID.fromString(courseId), UUID.fromString(unitId), UUID.fromString(lessonId), fwCode);
        if (milestone != null) {
          loc.put(JsonConstants.MILESTONE_ID, milestone.get(AJEntityMilestone.MILESTONE_ID));
        } else {
          LOGGER.error("Milestone Id cannot be obtained for this course {} and framework {}",
              courseId, fwCode);
        }
      }
      currentLocArray.add(loc);

    } else {
      LOGGER.info("Current Location Attributes cannot be obtained");
    }

    resultBody.put(JsonConstants.CONTENT, currentLocArray).putNull(JsonConstants.MESSAGE)
        .putNull(JsonConstants.PAGINATE);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }
}
