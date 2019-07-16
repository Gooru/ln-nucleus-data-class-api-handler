package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa.cm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollection;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCoreContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityLesson;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityUserNavigationPaths;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityUserRoute0ContentDetail;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.validators.ValidationUtils;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class OAToTeacherGradeHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAToTeacherGradeHandler.class);
  private final ProcessorContext context;
  private String classId;
  private String courseId;
  JsonArray resultArray = new JsonArray();
  Map<String, Integer> oaStudentMap = new HashMap<>();
  Map<String, JsonObject> oaMap = new HashMap<>();

  public OAToTeacherGradeHandler(ProcessorContext context) {
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
      this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);
      if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(courseId)) {
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
    // List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.classId,
    // this.context.userIdFromSession());
    // if (owner.isEmpty()) {
    // LOGGER.debug("validateRequest() FAILED");
    // return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
    // "User is not authorized for OA Grading"), ExecutionStatus.FAILED);
    // }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    List<Map> oaList = Base.findAll(AJEntityOACompletionStatus.GET_CM_OA_TO_TEACHER_GRADE, this.classId, this.courseId);
    if (!oaList.isEmpty()) {
      oaList.forEach(m -> {
        String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
        JsonObject oaObject = new JsonObject();
        oaObject.put(AJEntityBaseReports.COLLECTION_OID, collectionId);
        oaObject.put(AJEntityBaseReports.COLLECTION_TYPE,
            m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
        oaObject.put(AJEntityBaseReports.ATTR_COURSE_ID,
            m.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
        oaObject.put(AJEntityBaseReports.ATTR_UNIT_ID,
            m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString());
        oaObject.put(AJEntityBaseReports.ATTR_LESSON_ID,
            m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString());
        Integer pathId = (m.get(AJEntityBaseReports.PATH_ID) != null
            && Integer.valueOf(m.get(AJEntityBaseReports.PATH_ID).toString()) > 0)
                ? Integer.valueOf(m.get(AJEntityBaseReports.PATH_ID).toString())
                : null;
        String pathType = m.get(AJEntityBaseReports.PATH_TYPE) != null
            ? m.get(AJEntityBaseReports.PATH_TYPE).toString()
            : null;
        oaObject.put(AJEntityBaseReports.ATTR_PATH_ID, pathId);
        oaObject.put(AJEntityBaseReports.ATTR_PATH_TYPE, pathType);
        if (!oaMap.containsKey(collectionId)) {
          oaMap.put(collectionId, oaObject);
        }
        calculateStudentCount(collectionId);

      });
      buildResponse();

    } else {
      LOGGER.info("CM Offline Activity pending grading cannot be obtained");
    }

    result.put(JsonConstants.GRADE_ITEMS, resultArray).put(AJEntityBaseReports.ATTR_CLASS_ID,
        this.classId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);

  }

  private void calculateStudentCount(String collectionId) {
    if (oaStudentMap.containsKey(collectionId)) {
      int studCount = oaStudentMap.get(collectionId) + 1;
      oaStudentMap.put(collectionId, studCount);
    } else {
      oaStudentMap.put(collectionId, 1);
    }
  }

  private void buildResponse() {
    if (!oaMap.isEmpty()) {
      for( Entry<String, JsonObject> oam : oaMap.entrySet()) {
        String key = oam.getKey(); 
        JsonObject val = oam.getValue();
        if (oaStudentMap.containsKey(key)) {
          JsonObject coll = new JsonObject();
          String collectionId = val.getString(AJEntityBaseReports.COLLECTION_OID);
          String pathType = val.getString(AJEntityBaseReports.ATTR_PATH_TYPE);
          Integer pathId = val.getInteger(AJEntityBaseReports.ATTR_PATH_ID);
          AJEntityCollection collection = null;
          try {
            collection = AJEntityCollection.fetchCollection(collectionId);
            if (collection == null) {
              continue;
            }
            // Get Unit/Lesson from Route0 table for Route0 Suggestions
            if ((pathId != null && pathId > 0)
                && (pathType != null && pathType.equalsIgnoreCase(AJEntityBaseReports.ROUTE0))) {
              coll = populateRoute0SuggestULCMetadata(coll, collectionId);
            } else {
              coll = checkIfSystemSuggestExistsAndPopulateULCMetadata(coll, pathId, pathType,
                  val.getString(AJEntityBaseReports.ATTR_LESSON_ID), collectionId);
            }
            // Either route0/system suggested content is unavailable or ULC are unavailable
            // (content may be moved to another container or deleted), so skip content.
            if (coll == null) {
              continue;
            }
            coll.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
            coll.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE,
                val.getString(AJEntityBaseReports.COLLECTION_TYPE));
            coll.put(JsonConstants.COLLECTION_TITLE,
                collection.getString(AJEntityCoreContent.TITLE));

          } catch (Throwable e1) {
            LOGGER.info("CMOATGH::Exception while fetching OA meta:: {}",
                e1.fillInStackTrace());
          }

          int studentCount =
              oaStudentMap.get(key) != null ? Integer.valueOf(oaStudentMap.get(key).toString()) : 0;
          coll.put(JsonConstants.STUDENT_COUNT, studentCount);
          resultArray.add(coll);
        }
      }
    }
  }

  private JsonObject populateRoute0SuggestULCMetadata(JsonObject coll, String collectionId)
      throws Throwable {
    AJEntityUserRoute0ContentDetail suggestedContent =
        AJEntityUserRoute0ContentDetail.fetchRoute0SuggestedContent(collectionId);
    if (suggestedContent == null) {
      return null;
    }
    coll.put(AJEntityBaseReports.ATTR_LESSON_ID,
        suggestedContent.get(AJEntityBaseReports.LESSON_GOORU_OID).toString());
    coll.put(JsonConstants.LESSON_TITLE,
        suggestedContent.get(AJEntityUserRoute0ContentDetail.LESSON_TITLE).toString());
    coll.put(AJEntityBaseReports.ATTR_UNIT_ID,
        suggestedContent.get(AJEntityBaseReports.UNIT_GOORU_OID).toString());
    coll.put(JsonConstants.UNIT_TITLE,
        suggestedContent.get(AJEntityUserRoute0ContentDetail.UNIT_TITLE).toString());
    AJEntityCollection collectionData = AJEntityCollection.fetchCollection(collectionId);
    if (collectionData == null) {
      return null;
    }
    coll.put(JsonConstants.COLLECTION_TITLE, collectionData.getString(AJEntityCoreContent.TITLE));
    return coll;
  }

  private JsonObject checkIfSystemSuggestExistsAndPopulateULCMetadata(JsonObject coll,
      Integer pathId, String pathType, String lessonId, String collectionId) throws Throwable {
    if (!isSystemSuggestExists(pathId, pathType, collectionId, lessonId)) {
      return null;
    }
    Map<?, ?> ulMeta = AJEntityLesson.fetchLesson(courseId, lessonId);
    if (ulMeta == null || ulMeta.isEmpty()) {
      return null;
    }
    setUnitLessonData(coll, ulMeta);
    AJEntityCollection collectionData =
        AJEntityCollection.fetchCollectionByLesson(collectionId, lessonId);
    if (collectionData == null) {
      return null;
    }
    coll.put(JsonConstants.COLLECTION_TITLE, collectionData.getString(AJEntityCoreContent.TITLE));
    return coll;
  }

  private Boolean isSystemSuggestExists(Integer pathId, String pathType, String collectionId,
      String lessonId) throws Throwable {
    Boolean isValidContent = true;
    if ((pathId != null && pathId > 0)
        && (pathType != null && pathType.equalsIgnoreCase(AJEntityBaseReports.SYSTEM))) {
      AJEntityUserNavigationPaths suggestedContent = AJEntityUserNavigationPaths
          .fetchSystemSuggestedContent(collectionId, classId, courseId, lessonId);
      if (suggestedContent == null)
        isValidContent = false;
    }
    return isValidContent;
  }

  private void setUnitLessonData(JsonObject que, Map<?, ?> ulMeta) throws Throwable {
    que.put(AJEntityBaseReports.ATTR_LESSON_ID,
        ulMeta.get(AJEntityBaseReports.LESSON_GOORU_OID).toString());
    que.put(AJEntityBaseReports.ATTR_UNIT_ID,
        ulMeta.get(AJEntityBaseReports.UNIT_GOORU_OID).toString());
    que.put(JsonConstants.LESSON_TITLE, ulMeta.getOrDefault(JsonConstants.LESSON_TITLE, null));
    que.put(JsonConstants.UNIT_TITLE, ulMeta.getOrDefault(JsonConstants.UNIT_TITLE, null));
  }


  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
