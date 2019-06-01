package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollection;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCoreContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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
 * @author renuka
 * 
 */
public class DCAOAToGradeHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAOAToGradeHandler.class);
  private final ProcessorContext context;
  private String classId;
  JsonArray resultArray = new JsonArray();
  Map<Long, Integer> oaStudentMap = new HashMap<>();
  Map<Long, JsonObject> oaMap = new HashMap<>();



  public DCAOAToGradeHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request recieved to fetch Offline Activity to grade");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Offline Activity to grade"), ExecutionStatus.FAILED);
    }
    this.classId = this.context.request().getString(MessageConstants.CLASS_ID);   
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
//    List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
//        this.context.request().getString(MessageConstants.CLASS_ID),
//        this.context.userIdFromSession());
//    if (owner.isEmpty()) {
//      LOGGER.debug("validateRequest() FAILED");
//      return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
//          "User is not authorized for Offline Activity Grading"), ExecutionStatus.FAILED);
//    }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    List<Map> queMap =
        Base.findAll(AJEntityDailyClassActivity.GET_OA_TO_GRADE, this.classId);
    if (!queMap.isEmpty()) {
      queMap.forEach(m -> {
        String collectionType = m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString();
        String collectionId = m.get(AJEntityDailyClassActivity.COLLECTION_OID).toString();
        String studentId = m.get(AJEntityDailyClassActivity.GOORUUID).toString();
        Long dcaContentId = Long.valueOf(m.get(AJEntityDailyClassActivity.DCA_CONTENT_ID).toString());        
      calculateStudentCount(collectionType, collectionId, dcaContentId);

      });
      buildResponse();
      
    } else {
      LOGGER.info("Offline Activity pending grading cannot be obtained");
    }

    result.put(JsonConstants.GRADE_ITEMS, resultArray).put(AJEntityDailyClassActivity.ATTR_CLASS_ID,
        this.classId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  private void calculateStudentCount(String collectionType, String collectionId, Long dcaContentId) {
    JsonObject oaObject = new JsonObject();
    oaObject.put(AJEntityDailyClassActivity.COLLECTION_OID, collectionId);
    oaObject.put(AJEntityDailyClassActivity.COLLECTION_TYPE, collectionType);
    oaObject.put(AJEntityDailyClassActivity.DCA_CONTENT_ID, dcaContentId);

    if (!oaMap.containsKey(dcaContentId)) {
      oaMap.put(dcaContentId, oaObject);
    }
    if (oaStudentMap.containsKey(dcaContentId)) {
      int studCount = oaStudentMap.get(dcaContentId) + 1;
      oaStudentMap.put(dcaContentId, studCount);
    } else {
      oaStudentMap.put(dcaContentId, 1);
    }
  }

  private void buildResponse() {
    if (!oaMap.isEmpty()) {
      oaMap.forEach((key, val) -> {
        if (oaStudentMap.containsKey(key)) {
          JsonObject coll = new JsonObject();
          String collId = val.getString(AJEntityDailyClassActivity.COLLECTION_OID); 
          String collTitle = fetchCollectionMeta(collId);
          coll.put(JsonConstants.TITLE, collTitle);
          coll.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, collId);
          coll.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE,
              val.getString(AJEntityDailyClassActivity.COLLECTION_TYPE));
          coll.put(AJEntityDailyClassActivity.ATTR_DCA_CONTENT_ID,
              val.getLong(AJEntityDailyClassActivity.DCA_CONTENT_ID));
          int studentCount = oaStudentMap.get(key) != null
              ? Integer.valueOf(oaStudentMap.get(key).toString())
              : 0;
          coll.put(JsonConstants.STUDENT_COUNT, studentCount);
          resultArray.add(coll);
        }
      });
    }
  }
  
  private String fetchCollectionMeta(String collectionId) {
    String title = EventConstants.NA;
    try {
      AJEntityCollection collectionData =
          AJEntityCollection.fetchCollectionMeta(collectionId);
      if (collectionData != null) {
        title = collectionData.getString(AJEntityCoreContent.TITLE);
      }      
    } catch (Throwable e1) {
      LOGGER.error("Exception while fetching Collection metadata:: {}",
          e1.fillInStackTrace());
    } 
    return title;

  }
  
  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
