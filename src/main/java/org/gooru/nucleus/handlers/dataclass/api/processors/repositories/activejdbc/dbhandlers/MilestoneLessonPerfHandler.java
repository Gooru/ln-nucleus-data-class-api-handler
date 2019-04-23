package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityMilestoneLessonMap;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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
public class MilestoneLessonPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MilestoneLessonPerfHandler.class);
  private static final String REQUEST_COLLECTION_TYPE = "collectionType";
  private static final String FRAMEWORK_CODE = "fwCode";
  private static final String REQUEST_USERID = "userUid";
  int totalCount = 0;
  private final ProcessorContext context;
  private String collectionType;
  private String lessonId;
  private String fwCode;

  public MilestoneLessonPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Student Performance in Milestone");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid data provided to fetch Student Performance in Milestones"),
          ExecutionStatus.FAILED);
    } else if (context.request() != null && !context.request().isEmpty()) {
      this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
      if (StringUtil.isNullOrEmpty(collectionType)) {
        LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Milestones");
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse(
                "CollectionType Missing. Cannot fetch Student Performance in Milestones"),
            ExecutionStatus.FAILED);
      }

      this.fwCode = this.context.request().getString(FRAMEWORK_CODE);
      if (StringUtil.isNullOrEmpty(fwCode)) {
        LOGGER.warn("Framework Code is mandatory to fetch Student Performance in Milestones");
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse(
                "Framework Code Missing. Cannot fetch Student Performance in Milestones"),
            ExecutionStatus.FAILED);
      }
    }
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          this.context.classId(), this.context.userIdFromSession());
      if (owner.isEmpty()) {
        LOGGER.debug("validateRequest() FAILED");
        return new ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
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
    Map<String, Integer> lessonAssessmentCountMap = new HashMap<String, Integer>();
    Map<String, Integer> lessonCollectionCountMap = new HashMap<String, Integer>();

    String userId = this.context.request().getString(REQUEST_USERID);
    String addCollTypeFilterToQuery = AJEntityBaseReports.ADD_COLL_TYPE_FILTER_TO_QUERY;
    if (!this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
      addCollTypeFilterToQuery = AJEntityBaseReports.ADD_ASS_TYPE_FILTER_TO_QUERY;
    }

    List<String> userIds = new ArrayList<>();
    List<String> lessonIds = new ArrayList<>();

    // Select Distinct Lesson IDs from this Milestone
    LazyList<AJEntityMilestoneLessonMap> lessonIDforMilestone;
    lessonIDforMilestone = AJEntityMilestoneLessonMap.findBySQL(
        AJEntityMilestoneLessonMap.SELECT_DISTINCT_LESSON_ID_FOR_MILESTONE_ID,
        UUID.fromString(context.courseId()), context.milestoneId(), fwCode);
    lessonIDforMilestone
        .forEach(lesson -> lessonIds.add(lesson.getString(AJEntityMilestoneLessonMap.LESSON_ID)));

    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn(
          "UserID is not in the request to fetch Student Performance in Milestones. Assume user is a teacher");
      LazyList<AJEntityBaseReports> userIdforMilestones = AJEntityBaseReports.findBySQL(
          AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_MILESTONE + addCollTypeFilterToQuery,
          context.classId(), context.courseId(), listToPostgresArrayString(lessonIds));
      userIdforMilestones
          .forEach(lesson -> userIds.add(lesson.getString(AJEntityBaseReports.GOORUUID)));
    } else {
      userIds.add(userId);
    }

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      JsonArray MilestoneDataArray = new JsonArray();

      if (!lessonIDforMilestone.isEmpty()) {
        List<Map> lessonDataList;
        if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
          lessonDataList = Base.findAll(
              AJEntityBaseReports.SELECT_STUDENT_MILESTONE_LESSON_PERF_FOR_COLLECTION,
              context.classId(), context.courseId(), userID, listToPostgresArrayString(lessonIds));
        } else {
          lessonDataList =
              Base.findAll(AJEntityBaseReports.SELECT_STUDENT_MILESTONE_LESSON_PERF_FOR_ASSESSMENT,
                  context.classId(), context.courseId(), userID,
                  listToPostgresArrayString(lessonIds), EventConstants.COLLECTION_PLAY);
        }
        if (!lessonDataList.isEmpty()) {
          for (Map m : lessonDataList) {
            this.lessonId = m.get(AJEntityBaseReports.ATTR_LESSON_ID).toString();
            List<Map> scoreCompletionMap;
            List<Map> scoreMap = null;
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              scoreCompletionMap =
                  Base.findAll(AJEntityBaseReports.GET_COLL_COMPLETED_COUNT_SCORE_FOREACH_LESSON_ID,
                      context.classId(), context.courseId(), this.lessonId, userID,
                      EventConstants.COLLECTION_PLAY);
              scoreMap =
                  Base.findAll(AJEntityBaseReports.GET_MILESTONE_COLL_SCORE_FOREACH_LESSON_ID,
                      context.classId(), context.courseId(), this.lessonId, userID);
            } else {
              scoreCompletionMap =
                  Base.findAll(AJEntityBaseReports.GET_ASMT_COMPLETED_COUNT_SCORE_FOREACH_LESSON_ID,
                      context.classId(), context.courseId(), this.lessonId, userID,
                      EventConstants.COLLECTION_PLAY, EventConstants.STOP);
            }
            JsonObject lessonData = ValueMapper
                .map(ResponseAttributeIdentifier.getMilestoneLessonPerformanceAttributesMap(), m);
            scoreCompletionMap.forEach(sc -> {
              lessonData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                  Integer.valueOf(sc.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
              lessonData.put(AJEntityBaseReports.ATTR_SCORE,
                  sc.get(AJEntityBaseReports.ATTR_SCORE) != null
                      ? Math
                          .round(Double.valueOf(sc.get(AJEntityBaseReports.ATTR_SCORE).toString()))
                      : null);
              lessonData.put(AJEntityBaseReports.ATTR_UNIT_ID,
                  sc.get(AJEntityBaseReports.ATTR_UNIT_ID) != null
                      ? sc.get(AJEntityBaseReports.ATTR_UNIT_ID).toString()
                      : null);
            });

            if (scoreMap != null && !scoreMap.isEmpty()
                && this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              scoreMap.forEach(score -> {
                double maxScore =
                    Double.valueOf(score.get(AJEntityBaseReports.MAX_SCORE).toString());
                if (maxScore > 0 && (score.get(AJEntityBaseReports.SCORE) != null)) {
                  double sumOfScore =
                      Double.valueOf(score.get(AJEntityBaseReports.SCORE).toString());
                  LOGGER.debug("maxScore : {} , sumOfScore : {} ", maxScore, sumOfScore);
                  lessonData.put(AJEntityBaseReports.ATTR_SCORE,
                      Math.round((sumOfScore / maxScore) * 100));
                } else {
                  lessonData.putNull(AJEntityBaseReports.ATTR_SCORE);
                }
              });
            }

            if (lessonAssessmentCountMap.containsKey(this.lessonId)) {
              totalCount = lessonAssessmentCountMap.get(this.lessonId);
            } else {
              Object classTotalCount = Base.firstCell(
                  AJEntityCourseCollectionCount.GET_MILESTONE_LESSON_ASSESSMENT_COUNT,
                  context.courseId(), this.lessonId);
              totalCount =
                  classTotalCount != null ? (Integer.valueOf(classTotalCount.toString())) : 0;
              lessonAssessmentCountMap.put(this.lessonId, totalCount);
            }

            lessonData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount);
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              lessonData.put(EventConstants.VIEWS, lessonData.getInteger(EventConstants.ATTEMPTS));
              lessonData.remove(EventConstants.ATTEMPTS);

              if (lessonCollectionCountMap.containsKey(this.lessonId)) {
                totalCount = lessonCollectionCountMap.get(this.lessonId);
              } else {
                Object classTotalCount = Base.firstCell(
                    AJEntityCourseCollectionCount.GET_MILESTONE_LESSON_COLLECTION_COUNT,
                    context.courseId(), this.lessonId);
                totalCount =
                    classTotalCount != null ? (Integer.valueOf(classTotalCount.toString())) : 0;
                lessonCollectionCountMap.put(this.lessonId, totalCount);
              }

              lessonData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount);
            } else if (this.collectionType.equalsIgnoreCase(EventConstants.ASSESSMENT)) {
              if (lessonAssessmentCountMap.containsKey(this.lessonId)) {
                totalCount = lessonAssessmentCountMap.get(this.lessonId);
              } else {
                Object classTotalCount = Base.firstCell(
                    AJEntityCourseCollectionCount.GET_MILESTONE_LESSON_ASSESSMENT_COUNT,
                    context.courseId(), this.lessonId);
                totalCount =
                    classTotalCount != null ? (Integer.valueOf(classTotalCount.toString())) : 0;
                lessonAssessmentCountMap.put(this.lessonId, totalCount);
              }

              lessonData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount);
            }
            MilestoneDataArray.add(lessonData);
          }
        } else {
          LOGGER.info("No data returned for Student Performance for Milestone");
        }

      } else {
        LOGGER.info("Could not get Student Milestone Performance");
      }
      contentBody.put(JsonConstants.USAGE_DATA, MilestoneDataArray).put(JsonConstants.USERUID,
          userID);
      resultarray.add(contentBody);
    }
    resultBody.put(JsonConstants.CONTENT, resultarray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
        ExecutionStatus.SUCCESSFUL);
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
