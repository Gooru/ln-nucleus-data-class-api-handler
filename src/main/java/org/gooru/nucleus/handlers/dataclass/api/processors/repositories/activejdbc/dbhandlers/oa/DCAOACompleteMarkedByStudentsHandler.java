package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa;

import java.util.List;
import java.util.UUID;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.LazyList;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class DCAOACompleteMarkedByStudentsHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAOACompleteMarkedByStudentsHandler.class);
  public static final String DCA = "dailyclassactivity";
  private final ProcessorContext context;
  private String classId;
  private String oaId;
  private Long itemId;

  public DCAOACompleteMarkedByStudentsHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch OA complete Student Ids");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid request received to fetch OA complete Student Ids"), ExecutionStatus.FAILED);
    }

    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
    this.oaId = this.context.request().getString(MessageConstants.OA_ID);
    this.itemId = Long.valueOf(this.context.request().getString(MessageConstants.ITEM_ID));
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    /*List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.classId,
        this.context.userIdFromSession());
    if (owner.isEmpty()) {
      LOGGER.debug("validateRequest() FAILED");
      return new ExecutionResult<>(
          MessageResponseFactory.createForbiddenResponse("User is not authorized for OA Grading"),
          ExecutionStatus.FAILED);
    }*/

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultarray = new JsonArray();
    
    LazyList<Model> isOAMarkedCompleteByStudent = AJEntityOACompletionStatus.findBySQL(
        AJEntityOACompletionStatus.GET_OA_MARKED_AS_COMPLETE_BY_STUDENT, classId, oaId, itemId,
        DCA);
    if (!isOAMarkedCompleteByStudent.isEmpty()) {
      List<UUID> userIds =
          isOAMarkedCompleteByStudent.collect(AJEntityOACompletionStatus.STUDENT_ID);
      for (UUID userID : userIds) {
        LOGGER.debug("UID is " + userID);
        resultarray.add(userID.toString());
      }
    } else {
      LOGGER.info("OA Complete Student Ids cannot be obtained");
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
