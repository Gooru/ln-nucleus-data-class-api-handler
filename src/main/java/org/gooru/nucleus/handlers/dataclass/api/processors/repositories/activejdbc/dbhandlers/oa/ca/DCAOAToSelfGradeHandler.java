package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa.ca;

import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollection;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCoreContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
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

public class DCAOAToSelfGradeHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAOAToSelfGradeHandler.class);
  private final ProcessorContext context;
  private String classId;
  private String studentId;
  JsonArray resultArray = new JsonArray();

  public DCAOAToSelfGradeHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request recieved to fetch Offline Activity to grade");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Offline Activity to grade"), ExecutionStatus.FAILED);
    } else if (context.request() != null || !context.request().isEmpty()) {
      this.classId = this.context.request().getString(MessageConstants.CLASS_ID);
      this.studentId = this.context.request().getString(MessageConstants.USER_ID);
      if (StringUtil.isNullOrEmpty(classId) || StringUtil.isNullOrEmpty(studentId))
          {
        LOGGER.warn("Invalid Json Payload");
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
            ExecutionStatus.FAILED);
      }     
    }
       
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
//    if (StringUtil.isNullOrEmpty(this.context.userIdFromSession())) {
//      return new
//          ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
//          ("Auth Failure"), ExecutionStatus.FAILED);
//    } else {
//      if (!this.context.userIdFromSession()
//          .equals(studentId)) {
//        return new
//            ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
//            ("Auth Failure"), ExecutionStatus.FAILED);
//      }
//    }  
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    List<Map> oaList = Base.findAll(AJEntityOACompletionStatus.GET_CA_OA_TO_SELF_GRADE, this.classId, this.studentId);
    if (oaList != null && !oaList.isEmpty()) {
      for (Map m : oaList) {        
        JsonObject coll = new JsonObject();
        String collectionId = m.get(AJEntityDailyClassActivity.COLLECTION_OID).toString();        
        String collTitle = fetchCollectionMeta(collectionId);
        coll.put(JsonConstants.TITLE, collTitle);
        coll.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, collectionId);
        coll.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE,
            m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
        coll.put(AJEntityDailyClassActivity.ATTR_DCA_CONTENT_ID,
            Long.valueOf(m.get(AJEntityOACompletionStatus.OA_DCA_ID).toString()));
        resultArray.add(coll);
      }
    } else {
      LOGGER.info("Offline Activity pending grading cannot be obtained for Student {}", studentId);
    }
    
    result.put(JsonConstants.GRADE_ITEMS, resultArray).put(EventConstants.CLASS_ID,
        this.classId).put(EventConstants.STUDENT_ID, this.studentId);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  private String fetchCollectionMeta(String collectionId) {
    String title = EventConstants.NA;
    try {
      AJEntityCollection collectionData = AJEntityCollection.fetchCollectionMeta(collectionId);
      if (collectionData != null) {
        title = collectionData.getString(AJEntityCoreContent.TITLE);
      }
    } catch (Throwable e1) {
      LOGGER.error("Exception while fetching Collection metadata:: {}", e1.fillInStackTrace());
    }
    return title;
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
