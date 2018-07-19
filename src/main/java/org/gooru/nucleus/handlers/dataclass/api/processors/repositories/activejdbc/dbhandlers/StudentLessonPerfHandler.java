package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
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
 * Modified by Daniel
 */

public class StudentLessonPerfHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentLessonPerfHandler.class);
	  private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userUid";
    private final ProcessorContext context;
    private String collectionType;
    private boolean isTeacher = false;

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
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
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
      LOGGER.warn("CollectionType is mandatory to fetch Student Performance in a Lesson");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in Lesson"),
              ExecutionStatus.FAILED);
    }

        String userId = this.context.request().getString(REQUEST_USERID);
    List<String> userIds = new ArrayList<>();

    // FIXME : userId can be added as GROUPBY in performance query. Not
    // necessary to get distinct users.
    if (context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
      isTeacher = true;
      LazyList<AJEntityBaseReports> userIdforlesson =
              AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_LESSON_ID_FILTERBY_COLLTYPE, context.classId(),
                      context.courseId(), context.unitId(), context.lessonId(), this.collectionType);
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));

    } else {
      isTeacher = false;
      userIds.add(userId);
    }

    LOGGER.debug("UID is " + userId);

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      JsonArray LessonKpiArray = new JsonArray();
      JsonArray altPathArray = new JsonArray();

      LazyList<AJEntityBaseReports> collIDforlesson;
      collIDforlesson = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_COLLID_FOR_LESSON_ID_FILTERBY_COLLTYPE, context.classId(),
              context.courseId(), context.unitId(), context.lessonId(), this.collectionType, userID);

      List<String> collIds = new ArrayList<>(collIDforlesson.size());
      if (!collIDforlesson.isEmpty()) {
        LOGGER.info("Got a list of Distinct collectionIDs for this lesson");
        collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
      }
      List<Map> assessmentKpi;
      if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
        assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION, context.classId(), context.courseId(),
                context.unitId(), context.lessonId(), listToPostgresArrayString(collIds), userID);
      } else {
        assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT, context.classId(), context.courseId(),
                context.unitId(), context.lessonId(), listToPostgresArrayString(collIds), userID, EventConstants.COLLECTION_PLAY);
      }
      if (!assessmentKpi.isEmpty()) {
        assessmentKpi.forEach(m -> {
          JsonObject lessonKpi = ValueMapper.map(ResponseAttributeIdentifier.getLessonPerformanceAttributesMap(), m);
          String cId = lessonKpi.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID);          
          lessonKpi.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 1);
          //In Gooru 3.0, total_count was hardcoded to 1 at this last mile, assessment/collection level
          //Replicating the same here.
          lessonKpi.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 1);
          
          if (this.collectionType.equalsIgnoreCase(JsonConstants.COLLECTION)) {
            List<Map> collectionQuestionCount;
            collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_SCORE_AND_MAX_SCORE, context.classId(), context.courseId(),
                    context.unitId(), context.lessonId(), cId , userID);
            collectionQuestionCount.forEach(score -> {
              double maxScore = Double.valueOf(score.get(AJEntityBaseReports.MAX_SCORE).toString());
              if(maxScore > 0 && (score.get(AJEntityBaseReports.SCORE) != null)) {
              	double sumOfScore = Double.valueOf(score.get(AJEntityBaseReports.SCORE).toString());                	
                  lessonKpi.put(AJEntityBaseReports.ATTR_SCORE, ((sumOfScore / maxScore) * 100));
              } else {
                lessonKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
              }
            });

            lessonKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, cId);
            lessonKpi.remove(AJEntityBaseReports.ATTR_ASSESSMENT_ID);
            lessonKpi.put(EventConstants.VIEWS, lessonKpi.getInteger(EventConstants.ATTEMPTS));
            lessonKpi.remove(EventConstants.ATTEMPTS);
          }else{
            lessonKpi.put(AJEntityBaseReports.ATTR_SCORE, m.get(AJEntityBaseReports.ATTR_SCORE) != null ?
            		Math.round(Double.valueOf(m.get(AJEntityBaseReports.ATTR_SCORE).toString())) : null);
            if (!isTeacher){
            	List<Map> resourceKpi;
                resourceKpi = Base.findAll(AJEntityBaseReports.GET_RESOURCE_PERF, context.classId(), context.courseId(),
                        context.unitId(), context.lessonId(), cId, userID, EventConstants.COLLECTION_RESOURCE_PLAY);
                if (!resourceKpi.isEmpty()) {
                    resourceKpi.forEach(res -> {
                    	JsonObject resKpi = new JsonObject();
                    	resKpi.put(AJEntityBaseReports.ATTR_ASSESSMENT_ID, cId);
                    	resKpi.put(AJEntityBaseReports.ATTR_PATH_ID, res.get(AJEntityBaseReports.ATTR_PATH_ID).toString());
                    	resKpi.put(AJEntityBaseReports.ATTR_RESOURCE_ID, res.get(AJEntityBaseReports.ATTR_RESOURCE_ID).toString());
                		resKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(res.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
                		resKpi.put(AJEntityBaseReports.VIEWS, Integer.parseInt(res.get(AJEntityBaseReports.VIEWS).toString()));
                		altPathArray.add(resKpi);
                      });
                }
            }
          }
          
          String gradeStatus = JsonConstants.COMPLETE;
          String latestSessionId = m.get(AJEntityBaseReports.SESSION_ID) != null ? m.get(AJEntityBaseReports.SESSION_ID).toString() : null;
          //Check grading completion with latest session id
          if (latestSessionId != null) {
              List<Map> inprogressListOfGradeStatus = Base.findAll(AJEntityBaseReports.FETCH_INPROGRESS_GRADE_STATUS_BY_SESSION_ID, userID, latestSessionId, cId);
              if (inprogressListOfGradeStatus != null && !inprogressListOfGradeStatus.isEmpty()) gradeStatus = JsonConstants.IN_PROGRESS;
          }
          lessonKpi.put(AJEntityBaseReports.ATTR_GRADE_STATUS, gradeStatus);
          LessonKpiArray.add(lessonKpi);
        });
      } else {
        LOGGER.debug("No data returned for Student Perf in Assessment");
      }
      contentBody.put(JsonConstants.USAGE_DATA, LessonKpiArray).put(JsonConstants.ALTERNATE_PATH, altPathArray).put(JsonConstants.USERUID, userID);
      resultarray.add(contentBody);
    }
    resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

  }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }
    private String listToPostgresArrayString(List<String> input) {
      int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
                                                  // 36
                                                  // chars
      Iterator<String> it = input.iterator();
      if (!it.hasNext()) {
        return "{}";
      }

      StringBuilder sb = new StringBuilder(approxSize);
      sb.append('{');
      for (;;) {
        String s = it.next();
        sb.append('"').append(s).append('"');
        if (!it.hasNext()) {
          return sb.append('}').toString();
        }
        sb.append(',');
      }

    }
}
