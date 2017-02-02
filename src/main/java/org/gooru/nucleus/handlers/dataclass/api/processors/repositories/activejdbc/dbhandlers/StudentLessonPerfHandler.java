package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

public class StudentLessonPerfHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentLessonPerfHandler.class);
	  private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userUid";
    private final ProcessorContext context;
    private String collectionType;
    private String userId;

    public StudentLessonPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Lessons");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Lessons"),
                ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.getUserIdFromRequest() == null
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> creator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
        if (creator.isEmpty()) {
          List<Map> collaborator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_COLLABORATOR, this.context.classId(), this.context.userIdFromSession());
          if (collaborator.isEmpty()) {
            LOGGER.debug("validateRequest() FAILED");
            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
          }
        }
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
      JsonObject resultBody = new JsonObject();
      JsonArray resultarray = new JsonArray();
  
      // CollectionType is a Mandatory Parameter
      this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
      if (StringUtil.isNullOrEmpty(collectionType)) {
        LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Course");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in course"),
                ExecutionStatus.FAILED);
      }
      LOGGER.debug("Collection Type is " + this.collectionType);
  
      this.userId = this.context.request().getString(REQUEST_USERID);
      List<String> userIds = new ArrayList<>();
  
      // FIXME : userId can be added as GROUPBY in performance query. Not
      // necessary to get distinct users.
      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
        LazyList<AJEntityBaseReports> userIdforlesson =
                AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_LESSONID_FILTERBY_COLLTYPE, context.classId(),
                        context.courseId(), context.unitId(), context.lessonId(), this.collectionType);
        userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));
  
      } else {
        userIds.add(this.userId);
      }
  
      LOGGER.debug("UID is " + this.userId);
  
      for (String userID : userIds) {
        JsonObject contentBody = new JsonObject();
        JsonArray LessonKpiArray = new JsonArray();
        List<Map> assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT, context.classId(), context.courseId(),
                context.unitId(), context.lessonId(), this.collectionType, userID);
        if (!assessmentKpi.isEmpty()) {
          assessmentKpi.forEach(m -> {
            JsonObject lessonKpi = ValueMapper.map(ResponseAttributeIdentifier.getLessonPerformanceAttributesMap(), m);
            // FIXME : revisit completed count and total count
            lessonKpi.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 1);
            lessonKpi.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
            // FIXME: This logic to be revisited.
            if (this.collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
              lessonKpi.put(AJEntityBaseReports.ATTR_ASSESSMENT_ID, lessonKpi.getString(AJEntityBaseReports.ATTR_COLLECTION_ID));
              lessonKpi.remove(AJEntityBaseReports.ATTR_COLLECTION_ID);
            }
            LessonKpiArray.add(lessonKpi);
          });
        } else {
          // Return an empty resultBody instead of an Error
          LOGGER.debug("No data returned for Student Perf in Assessment");
        }
        contentBody.put(JsonConstants.USAGE_DATA, LessonKpiArray).put(JsonConstants.USERUID, userID);
        resultarray.add(contentBody);
      }
      resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
  
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
  
    }   

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
}
