package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.oa.cm;

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
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityLesson;
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

public class OAToSelfGradeHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAToSelfGradeHandler.class);
  private final ProcessorContext context;
  private String classId;
  private String studentId;
  private String courseId;
  JsonArray resultArray = new JsonArray();

  public OAToSelfGradeHandler(ProcessorContext context) {
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
      this.courseId = this.context.request().getString(MessageConstants.COURSE_ID);
      if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(courseId)
          || !ValidationUtils.isValidUUID(studentId)) {
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
    // if (StringUtil.isNullOrEmpty(this.context.userIdFromSession())) {
    // return new
    // ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
    // ("Auth Failure"), ExecutionStatus.FAILED);
    // } else {
    // if (!this.context.userIdFromSession()
    // .equals(studentId)) {
    // return new
    // ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
    // ("Auth Failure"), ExecutionStatus.FAILED);
    // }
    // }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    List<Map> oaList = Base.findAll(AJEntityBaseReports.GET_OA_TO_SELF_GRADE, this.classId,
        this.courseId, this.studentId);
    if (oaList != null && !oaList.isEmpty()) {
      generateResponse(oaList);
    } else {
      LOGGER.info("Offline Activity pending grading cannot be obtained for Student {}", studentId);
    }

    result.put(JsonConstants.GRADE_ITEMS, resultArray).put(EventConstants.CLASS_ID, this.classId)
        .put(EventConstants.STUDENT_ID, this.studentId);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  @SuppressWarnings("rawtypes")
  private void generateResponse(List<Map> oaList) {
    for (Map m : oaList) {
      JsonObject coll = new JsonObject();
      String collectionId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
      Integer pathId = (m.get(AJEntityBaseReports.PATH_ID) != null
          && Integer.valueOf(m.get(AJEntityBaseReports.PATH_ID).toString()) > 0)
              ? Integer.valueOf(m.get(AJEntityBaseReports.PATH_ID).toString())
              : null;
      String pathType = m.get(AJEntityBaseReports.PATH_TYPE) != null
          ? m.get(AJEntityBaseReports.PATH_TYPE).toString()
          : null;
      
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
              m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString(), collectionId);
        }
        // Either route0/system suggested content is unavailable or ULC are unavailable
        // (content may be moved to another container or deleted), so skip content.
        if (coll == null) {
          continue;
        }
        coll.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
        coll.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE,
            m.get(AJEntityBaseReports.COLLECTION_TYPE).toString());
        coll.put(JsonConstants.COLLECTION_TITLE,
            collection.getString(AJEntityCoreContent.TITLE));

      } catch (Throwable e1) {
        LOGGER.info("CMOATGH::Exception while fetching OA meta:: {}",
            e1.fillInStackTrace());
      }
      resultArray.add(coll);
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
