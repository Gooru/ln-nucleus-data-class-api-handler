package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudentResourceCurrentLocationHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StudentResourceCurrentLocationHandler.class);

  private final ProcessorContext context;
  private String userId;
  private String classId;
  private Long pathId;
  private String collectionId;
  private String courseId;
  private String unitId;
  private String lessonId;
  private String source;
  private String pathType;
  private Long dcaContentId;

  public StudentResourceCurrentLocationHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    try {
      validateContextRequest();
      initializeRequestParams();
      validateContextRequestFields();
    } catch (MessageResponseWrapperException mrwe) {
      return new ExecutionResult<>(mrwe.getMessageResponse(),
          ExecutionResult.ExecutionStatus.FAILED);
    }
    LOGGER.debug("checkSanity() OK validate");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  private void validateContextRequest() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Resource Current Location");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch Student Resource Current Location"));
    }
  }

  private void initializeRequestParams() {
    this.userId = context.getUserIdFromRequest();
    this.collectionId = context.collectionId();
    this.classId = context.request().getString(JsonConstants.CLASS_ID);
    this.courseId = context.request().getString(JsonConstants.COURSE_ID);
    this.unitId = context.request().getString(JsonConstants.UNIT_ID);
    this.lessonId = context.request().getString(JsonConstants.LESSON_ID);
    this.pathType = context.request().getString(JsonConstants.PATH_TYPE);


  }

  private void validateContextRequestFields() {
    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("userId is mandatory to fetch Student Resource Current Location.");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "userId is Missing. Cannot fetch Student Student Resource Current Location"));
    }
    String pId = this.context.request().getString(JsonConstants.PATH_ID);
    if (!StringUtil.isNullOrEmpty(pId)) {
      try {
        Long path = Long.valueOf(pId.toString());
        this.pathId = (path > 0) ? path : null;
      } catch (NumberFormatException nfe) {
        throw new MessageResponseWrapperException(
            MessageResponseFactory.createInvalidRequestResponse(
                "NumberFormatException:Invalid pathIds provided to fetch Student Resource Current Location"));
      }
    }

    String contentSource = this.context.request().getString(JsonConstants.SOURCE);
    String dcaId = this.context.request().getString(JsonConstants.DCA_CONTENT_ID);
    String classId = this.context.request().getString(JsonConstants.CLASS_ID);
    if (!StringUtil.isNullOrEmpty(contentSource)) {
      this.source = contentSource;
      if (contentSource.equalsIgnoreCase(JsonConstants.CA_SOURCE)) {
        if (!StringUtil.isNullOrEmpty(dcaId) && !StringUtil.isNullOrEmpty(classId)) {
          LOGGER.warn("dcaContentId is mandatory to fetch Student Resource Current Location");
          this.classId = classId;
          try {
            this.dcaContentId = Long.valueOf(dcaId.toString());
          } catch (NumberFormatException nfe) {
            throw new MessageResponseWrapperException(
                MessageResponseFactory.createInvalidRequestResponse(
                    "NumberFormatException:Invalid pathIds provided to fetch Student Resource Current Location"));
          }
        } else {
          throw new MessageResponseWrapperException(MessageResponseFactory
              .createInvalidRequestResponse("Invalid data provided for source"));
        }
      }

    }

    if (StringUtil.isNullOrEmpty(collectionId)) {
      LOGGER.warn("CollectionId is mandatory to fetch Student Resource Current Location");
      throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse(
          "CollectionId is Missing. Cannot fetch Student Resource Current Location"));
    }
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  private static <T> Object[] mergeArray(T[] arr1, T[] arr2) {
    return Stream.of(arr1, arr2).flatMap(Stream::of).toArray();
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    StringBuilder query =
        new StringBuilder(" actor_id = ? and collection_id = ? and event_name = ? ");
    List<String> listOfStringParams = new ArrayList<>();
    List<Long> listOfLongParams = new ArrayList<>();
    JsonObject resultBody = new JsonObject();
    JsonArray currentLocArray = new JsonArray();
    listOfStringParams.add(userId);
    listOfStringParams.add(collectionId);
    listOfStringParams.add(EventConstants.COLLECTION_RESOURCE_PLAY);
    boolean isSourceCA = false;
    AJEntityBaseReports locModel = null;
    AJEntityDailyClassActivity dcaModel = null;
    if (!StringUtil.isNullOrEmpty(classId)) {
      query.append(" and class_id = ? ");
      listOfStringParams.add(classId);
    }
    if (!StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId)
        && !StringUtil.isNullOrEmpty(lessonId)) {
      query.append(" and course_id = ? and  unit_id= ? and lesson_id = ? ");
      listOfStringParams.add(courseId);
      listOfStringParams.add(unitId);
      listOfStringParams.add(lessonId);

    }
    if (!StringUtil.isNullOrEmpty(pathType)) {
      query.append(" and path_type = ? ");
      listOfStringParams.add(pathType);
    }
    if (!StringUtil.isNullOrEmpty(source)) {
      if (!source.equalsIgnoreCase(JsonConstants.CA_SOURCE)) {
        query.append(" and content_source = ? ");
        listOfStringParams.add(source);
      } else {
        isSourceCA = true;
        query.append(" and content_source = ?  and dca_content_id = ?  ");
        listOfStringParams.add(JsonConstants.CA_SOURCE_NAME);
        listOfLongParams.add(dcaContentId);
      }
    } else {
      query.append(" and content_source is null ");

    }
    if (pathId != null) {
      query.append(" and path_id = ? ");
      listOfLongParams.add(pathId);
    }

    query.append(" ORDER BY updated_at DESC ");
    Object[] allParams = mergeArray(listOfStringParams.toArray(), listOfLongParams.toArray()); // merged
                                                                                               // array
    LOGGER.debug("query: {} params: {} ", query, allParams);
    
    if (isSourceCA) {
      dcaModel = AJEntityDailyClassActivity.findFirst(query.toString(), allParams);
    } else {
      locModel = AJEntityBaseReports.findFirst(query.toString(), allParams);
    }
    if (locModel != null) {
      JsonObject loc = new JsonObject();
      loc.put(AJEntityBaseReports.COLLECTION_OID, collectionId);
      String resourceId = locModel.get(AJEntityBaseReports.RESOURCE_ID) != null
          ? locModel.get(AJEntityBaseReports.RESOURCE_ID).toString()
          : null;
      loc.put(AJEntityBaseReports.RESOURCE_ID, resourceId);
      AJEntityBaseReports collectionStatus = AJEntityBaseReports.findFirst(
          "session_id = ?  AND collection_id = ? AND event_name = ? AND event_type = ?",
          locModel.get(AJEntityBaseReports.SESSION_ID).toString(), collectionId,
          EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP);
      if (collectionStatus != null) {
        loc.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
      } else {
        loc.put(JsonConstants.STATUS, JsonConstants.IN_PROGRESS);
      }
      currentLocArray.add(loc);
    } else if (dcaModel != null) {
      JsonObject dca = new JsonObject();
      dca.put(AJEntityDailyClassActivity.COLLECTION_OID, collectionId);
      String resourceId = dcaModel.get(AJEntityDailyClassActivity.RESOURCE_ID) != null
          ? dcaModel.get(AJEntityDailyClassActivity.RESOURCE_ID).toString()
          : null;
      dca.put(AJEntityDailyClassActivity.RESOURCE_ID, resourceId);
      currentLocArray.add(dca);
      LOGGER.debug("currentLocArray {}", currentLocArray);
    } else {
      LOGGER.info("Current Location Attributes cannot be obtained");
    }

    resultBody.put(JsonConstants.CONTENT, currentLocArray).putNull(JsonConstants.MESSAGE)
        .putNull(JsonConstants.PAGINATE);
    LOGGER.debug("Current Location  obtained");
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
