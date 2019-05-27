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
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassID is mandatory to fetch Offline Activity pending grading");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Class Id Missing. Cannot fetch Offline Activity pending grading"), ExecutionStatus.FAILED);

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
          "User is not authorized for Offline Activity Grading"), ExecutionStatus.FAILED);
    }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray resultArray = new JsonArray();
    Map<String, Integer> oaStudentMap = new HashMap<>();
    Map<String, JsonObject> sourceToRelay = new HashMap<>();

    List<Map> queMap =
        Base.findAll(AJEntityDailyClassActivity.GET_OA_TO_GRADE, this.classId);
    if (!queMap.isEmpty()) {
      queMap.forEach(m -> {
        String collectionType = m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString();
        String collectionId = m.get(AJEntityDailyClassActivity.COLLECTION_OID).toString();
        String studentId = m.get(AJEntityDailyClassActivity.GOORUUID).toString();
        Date activityDate = Date.valueOf(m.get(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE).toString());

        calculateStudentCountAndGenerateSourceToRelay(oaStudentMap, sourceToRelay, collectionType,
            collectionId, activityDate);
      });
      buildResponse(resultArray, oaStudentMap, sourceToRelay);
    } else {
      LOGGER.info("Offline Activity pending grading cannot be obtained");
    }

    result.put(JsonConstants.GRADE_ITEMS, resultArray).put(AJEntityDailyClassActivity.ATTR_CLASS_ID,
        this.classId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  private void calculateStudentCountAndGenerateSourceToRelay(Map<String, Integer> oaStudentMap,
      Map<String, JsonObject> sourceToRelay, String collectionType, String collectionId,
      Date activityDate) {
    JsonObject oaObject = new JsonObject();
    oaObject.put(AJEntityDailyClassActivity.COLLECTION_OID, collectionId);
    oaObject.put(AJEntityDailyClassActivity.COLLECTION_TYPE, collectionType);
    oaObject.put(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE, activityDate.toString());

    if (!sourceToRelay.containsKey(collectionId.toString())) {
      sourceToRelay.put(collectionId, oaObject);
    }
    if (oaStudentMap.containsKey(collectionId)) {
      int studCount = oaStudentMap.get(collectionId) + 1;
      oaStudentMap.put(collectionId, studCount);
    } else {
      oaStudentMap.put(collectionId, 1);
    }
  }

  private void buildResponse(JsonArray resultArray, Map<String, Integer> oaStudentMap,
      Map<String, JsonObject> oaMap) {
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
          coll.put(AJEntityDailyClassActivity.ACTIVITY_DATE,
              val.getString(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE));
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
