package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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

/**
 * @author mukul@gooru
 * 
 */
public class DCAStudentsForRubricQuestionsHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DCAStudentsForRubricQuestionsHandler.class);
  private final ProcessorContext context;
  private String classId;
  private String collectionId;
  private Date activityDate;

  public DCAStudentsForRubricQuestionsHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Ids for Question to Grade");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid request received to fetch Student Ids for Question to Grade"),
          ExecutionStatus.FAILED);
    }

    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassID is mandatory to fetch student list for this question");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch student list"), ExecutionStatus.FAILED);

    }

    this.collectionId = this.context.request().getString(MessageConstants.COLLECTION_ID);
    if (StringUtil.isNullOrEmpty(collectionId)) {
      LOGGER.warn("CollectionID is mandatory to fetch student list");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Collection Id Missing. Cannot fetch student list"), ExecutionStatus.FAILED);

    }
    
    String date = this.context.request().getString(MessageConstants.ACTIVITY_DATE);
    if (StringUtil.isNullOrEmpty(date)) {
      LOGGER.warn("Activity Date is mandatory to fetch student list");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Activity Date is Missing. Cannot fetch student list"), ExecutionStatus.FAILED);
    } else {
      this.activityDate = Date.valueOf(date);
    }

    LOGGER.debug("checkSanity() OK");
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

    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultarray = new JsonArray();
    
    LazyList<AJEntityDailyClassActivity> userIdforQue = AJEntityDailyClassActivity.findBySQL(
        AJEntityDailyClassActivity.GET_DISTINCT_STUDENTS_FOR_THIS_RESOURCE, classId, collectionId,
        context.questionId(), activityDate);
    
    if (!userIdforQue.isEmpty()) {
      List<String> userIds = userIdforQue.collect(AJEntityDailyClassActivity.GOORUUID);
      for (String userID : userIds) {
        LOGGER.debug("UID is " + userID);
        AJEntityDailyClassActivity scoreModel = AJEntityDailyClassActivity.findFirst(
            "class_id = ? AND collection_id = ? AND resource_id = ? AND actor_id = ? "
                + "AND event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
                + "AND grading_type = 'teacher' AND  date_in_time_zone = ? AND question_type = 'OE' order by updated_at desc",
            classId, collectionId, context.questionId(), userID, activityDate);
        if (scoreModel != null) {
          Boolean isGraded = scoreModel.get(AJEntityDailyClassActivity.IS_GRADED) != null
              ? Boolean.valueOf(scoreModel.get(AJEntityDailyClassActivity.IS_GRADED).toString())
              : null;
          if (scoreModel.get(AJEntityDailyClassActivity.SCORE) == null
              && (isGraded != null && !isGraded)) {
            resultarray.add(userID);
          }
        }
      }

    } else {
      LOGGER.info("Student list for this Rubric Question cannot be obtained");
    }
    result.put(JsonConstants.STUDENTS, resultarray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }
}
