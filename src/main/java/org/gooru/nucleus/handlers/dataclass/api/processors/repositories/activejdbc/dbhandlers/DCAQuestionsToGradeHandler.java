package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;

import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollection;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCoreContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityLesson;
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
 * @author mukul@gooru
 * 
 */
public class DCAQuestionsToGradeHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAQuestionsToGradeHandler.class);
  private final ProcessorContext context;
  private String classId;


  public DCAQuestionsToGradeHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request recieved to fetch Questions to grade");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Questions to grade"), ExecutionStatus.FAILED);
    }
    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassID is mandatory to fetch Questions pending grading");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch Questions pending grading"), ExecutionStatus.FAILED);

    }    
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
     List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
     this.context.request().getString(MessageConstants.CLASS_ID),
     this.context.userIdFromSession());
     if (owner.isEmpty()) {
     LOGGER.debug("validateRequest() FAILED");
     return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
     "User is not authorized for Rubric Grading"), ExecutionStatus.FAILED);
     }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultArray = new JsonArray();
    Map<String, Integer> resourceStudentMap = new HashMap<>();
    Map<String, JsonObject> resourceCCULMap = new HashMap<>();

    List<Map> queMap =
        Base.findAll(AJEntityDailyClassActivity.GET_QUESTIONS_TO_GRADE, this.classId);
    if (!queMap.isEmpty()) {
      queMap.forEach(m -> {
        String collectionType = m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString();
        String collectionId = m.get(AJEntityDailyClassActivity.COLLECTION_OID).toString();
        String resourceId = m.get(AJEntityDailyClassActivity.RESOURCE_ID).toString();
        String sessionId = m.get(AJEntityDailyClassActivity.SESSION_ID).toString();
        String studentId = m.get(AJEntityDailyClassActivity.GOORUUID).toString();
        Date activityDate = Date.valueOf(m.get(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE).toString());

        JsonObject resCCULObject = new JsonObject();
        resCCULObject.put(AJEntityDailyClassActivity.COLLECTION_OID, collectionId);
        resCCULObject.put(AJEntityDailyClassActivity.COLLECTION_TYPE, collectionType);
        resCCULObject.put(AJEntityDailyClassActivity.RESOURCE_ID, resourceId);
        resCCULObject.put(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE, activityDate.toString());

        if (!resourceCCULMap.containsKey(resourceId+activityDate.toString())) {
          resourceCCULMap.put(resourceId+activityDate, resCCULObject);
        }
        // Find the latest Session Id for this User, for this resource in this CCUL
        AJEntityDailyClassActivity sessionModel = AJEntityDailyClassActivity.findFirst(
            "actor_id = ? AND class_id = ? AND collection_id = ? AND event_name = 'collection.play' "
                + "AND event_type = 'stop' AND date_in_time_zone = ? ORDER BY updated_at DESC",
            studentId, this.classId, collectionId, activityDate);
        if (sessionModel != null) {
          String latestSessionId = sessionModel.get(AJEntityDailyClassActivity.SESSION_ID) != null
              ? sessionModel.get(AJEntityDailyClassActivity.SESSION_ID).toString()
              : null;
          if (!StringUtil.isNullOrEmpty(latestSessionId)
              && latestSessionId.equals(sessionId.toString())) {
            if (resourceStudentMap.containsKey(resourceId+activityDate)) {
              int studCount = resourceStudentMap.get(resourceId+activityDate) + 1;
              resourceStudentMap.put(resourceId+activityDate, studCount);
            } else {
              resourceStudentMap.put(resourceId+activityDate, 1);
            }
          }
        }
      });
      if (!resourceCCULMap.isEmpty()) {
        resourceCCULMap.forEach((ky, val) -> {
          if (resourceStudentMap.containsKey(ky)) {
            JsonObject que = new JsonObject();
            que.put(AJEntityDailyClassActivity.ATTR_RESOURCE_ID, 
                val.getString(AJEntityDailyClassActivity.RESOURCE_ID));
            que.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID,
                val.getString(AJEntityDailyClassActivity.COLLECTION_OID));
            que.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE,
                val.getString(AJEntityDailyClassActivity.COLLECTION_TYPE));
            que.put(AJEntityDailyClassActivity.ACTIVITY_DATE,
                val.getString(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE));
            int studentCount = resourceStudentMap.get(ky) != null
                ? Integer.valueOf(resourceStudentMap.get(ky).toString())
                : 0;
            que.put(JsonConstants.STUDENT_COUNT, studentCount);
            resultArray.add(que);
          }
        });
      }
    } else {
      LOGGER.info("Questions pending grading cannot be obtained");
    }

    // TODO: Orchestrate Title etc
    result.put(JsonConstants.GRADE_ITEMS, resultArray).put(AJEntityDailyClassActivity.ATTR_CLASS_ID,
        this.classId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
