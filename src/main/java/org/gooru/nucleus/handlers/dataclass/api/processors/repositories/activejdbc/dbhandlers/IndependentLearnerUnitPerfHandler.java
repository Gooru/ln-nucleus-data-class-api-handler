package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
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
 * Created by Daniel
 */

public class IndependentLearnerUnitPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndependentLearnerUnitPerfHandler.class);
  private static final String REQUEST_COLLECTION_TYPE = "collectionType";
  private final ProcessorContext context;
  int totalCount = 0;
  private String collectionType;
  private String userId;

  // For stuffing Json
  private String lessonId;

  public IndependentLearnerUnitPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received to fetch Student Performance in Units");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Units"),
              ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    JsonObject resultBody = new JsonObject();
    JsonArray resultarray = new JsonArray();

    this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
    if (StringUtil.isNullOrEmpty(collectionType)) {
      LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Course");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in course"),
              ExecutionStatus.FAILED);
    }
    this.userId = this.context.userIdFromSession();
    List<String> userIds = new ArrayList<>(1);
    List<String> lessonIds = new ArrayList<>();

    userIds.add(this.userId);

    String addCollTypeFilterToQuery = AJEntityBaseReports.ADD_COLL_TYPE_FILTER_TO_QUERY;
    if (!this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) addCollTypeFilterToQuery = AJEntityBaseReports.ADD_ASS_TYPE_FILTER_TO_QUERY;

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      JsonArray UnitKpiArray = new JsonArray();
      Map<String, Integer> lessonAssessmentCountMap = new HashMap<String, Integer>();

      LazyList<AJEntityBaseReports> lessonIDforUnit;

      lessonIDforUnit = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_DISTINCT_LESSON_ID_FOR_UNIT_ID + addCollTypeFilterToQuery,
              context.courseId(), context.unitId(), userID);
      if (!lessonIDforUnit.isEmpty()) {
        LOGGER.debug("Got a list of Distinct lessonIDs for this Unit");

        lessonIDforUnit.forEach(lesson -> lessonIds.add(lesson.getString(AJEntityBaseReports.LESSON_GOORU_OID)));
        List<Map> lessonKpi;
        if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
          lessonKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_UNIT_PERF_FOR_COLLECTION, context.courseId(), context.unitId(),
                  userID, listToPostgresArrayString(lessonIds));
        } else {
          lessonKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_UNIT_PERF_FOR_ASSESSMENT, context.courseId(), context.unitId(),
                  userID, listToPostgresArrayString(lessonIds), EventConstants.COLLECTION_PLAY);
        }
        if (!lessonKpi.isEmpty()) {
          for (Map m : lessonKpi) {
            this.lessonId = m.get(AJEntityBaseReports.ATTR_LESSON_ID).toString();
            LOGGER.debug("The Value of LESSONID " + lessonId);
            List<Map> completedCountMap;
            List<Map> scoreMap = null;
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {

              completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLL_COUNT_FOREACH_INDEPENDENT_LEARNER_LESSON_ID, context.courseId(),
                      context.unitId(), this.lessonId, userID, EventConstants.COLLECTION_PLAY);
              scoreMap = Base.findAll(AJEntityBaseReports.GET_COLL_SCORE_FOREACH_IL_LESSON_ID,
                      context.courseId(), context.unitId(), this.lessonId, userID);
            } else {
              completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_ASMT_COUNT_FOREACH_INDEPENDENT_LEARNER_LESSON_ID, context.courseId(),
                      context.unitId(), this.lessonId, userID, EventConstants.COLLECTION_PLAY);
            }
            JsonObject lessonData = ValueMapper.map(ResponseAttributeIdentifier.getUnitPerformanceAttributesMap(), m);
            completedCountMap.forEach(scoreCompletonMap -> {
              lessonData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                      Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
              lessonData.put(AJEntityBaseReports.ATTR_SCORE, scoreCompletonMap.get(AJEntityBaseReports.ATTR_SCORE) != null ? 
            		  Double.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_SCORE).toString()) : null);
            });
            
            if(scoreMap != null && !scoreMap.isEmpty() && this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              scoreMap.forEach(score ->{
                double maxScore = Double.valueOf(score.get(AJEntityBaseReports.MAX_SCORE).toString());
                if(maxScore > 0 && (score.get(AJEntityBaseReports.SCORE) != null)) {
                  double sumOfScore = Double.valueOf(score.get(AJEntityBaseReports.SCORE).toString());
                    lessonData.put(AJEntityBaseReports.ATTR_SCORE, Math.round((sumOfScore / maxScore) * 100));
                } else {
                  lessonData.putNull(AJEntityBaseReports.ATTR_SCORE);
                }
              });
            }
            
            if (lessonAssessmentCountMap.containsKey(this.lessonId)) {
            	totalCount = lessonAssessmentCountMap.get(this.lessonId);
            } else {
            	Object classTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_LESSON_ASSESSMENT_COUNT,
            			context.courseId(), context.unitId(), this.lessonId);
            	totalCount = classTotalCount != null ? (Integer.valueOf(classTotalCount.toString())) : 0;
            	lessonAssessmentCountMap.put(this.lessonId, totalCount);
            }            lessonData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              lessonData.put(EventConstants.VIEWS, lessonData.getInteger(EventConstants.ATTEMPTS));
              lessonData.remove(EventConstants.ATTEMPTS);
            }
            JsonArray assessmentArray = new JsonArray();
            LazyList<AJEntityBaseReports> collIDforlesson;
            collIDforlesson = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_DISTINCT_COLLID_FOR_LESSON_ID_WO_PATH_ID + addCollTypeFilterToQuery,
                    context.courseId(), context.unitId(), this.lessonId, userID);

            List<String> collIds = new ArrayList<>();
            if (!collIDforlesson.isEmpty()) {
              LOGGER.info("Got a list of Distinct collectionIDs for this lesson");
              collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
            }
            List<Map> assessmentKpi;
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {

              assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_LESSON_PERF_FOR_COLLECTION_WO_PATH_ID, context.courseId(), context.unitId(),
                      this.lessonId, listToPostgresArrayString(collIds), userID);
            } else {
              assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_LESSON_PERF_FOR_ASSESSMENT_WO_PATH_ID, context.courseId(), context.unitId(),
                      this.lessonId, listToPostgresArrayString(collIds), userID, EventConstants.COLLECTION_PLAY);
            }
            if (!assessmentKpi.isEmpty()) {
              assessmentKpi.forEach(ass -> {
                JsonObject assData = ValueMapper.map(ResponseAttributeIdentifier.getLessonPerformanceAttributesMap(), ass);
                assData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 1);
                //Since this is the leaf-level data, TOTAL_COUNT doesn't make sense here. It will be stuffed as 1
                //for compatibility. (should not be used in any report calculations)
                assData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 1);
                String collId = assData.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID);
                if (this.collectionType.equalsIgnoreCase(JsonConstants.COLLECTION)) {
                  List<Map> collectionQuestionCount;
                    collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_IL_COLLECTION_SCORE_AND_MAX_SCORE,
                          context.courseId(), context.unitId(), this.lessonId, assData.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID),userID);
                  if (collectionQuestionCount != null && !collectionQuestionCount.isEmpty()) {
                    collectionQuestionCount.forEach(score -> {
                      double maxScore = Double.valueOf(score.get(AJEntityBaseReports.MAX_SCORE).toString());
                      if(maxScore > 0 && (score.get(AJEntityBaseReports.SCORE) != null)) {
                      double sumOfScore = Double.valueOf(score.get(AJEntityBaseReports.SCORE).toString());
                      LOGGER.debug("maxScore : {} , sumOfScore : {} ", maxScore, sumOfScore);
                        assData.put(AJEntityBaseReports.ATTR_SCORE, Math.round((sumOfScore / maxScore) * 100));
                      } else {
                        assData.putNull(AJEntityBaseReports.ATTR_SCORE);
                      }
                    });
                  } else {
                    assData.putNull(AJEntityBaseReports.ATTR_SCORE);
                  }
                  assData.put(AJEntityBaseReports.ATTR_COLLECTION_ID, assData.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID));
                  assData.remove(AJEntityBaseReports.ATTR_ASSESSMENT_ID);
                  assData.put(EventConstants.VIEWS, assData.getInteger(EventConstants.ATTEMPTS));
                  assData.remove(EventConstants.ATTEMPTS);
                
                }else {
                  assData.put(AJEntityBaseReports.ATTR_SCORE, ass.get(AJEntityBaseReports.ATTR_SCORE) != null ?  Math.round(Double.valueOf(ass.get(AJEntityBaseReports.ATTR_SCORE).toString())) : null);
                }
                
                String gradeStatus = JsonConstants.COMPLETE;
                String latestSessionId = m.get(AJEntityBaseReports.SESSION_ID) != null ? m.get(AJEntityBaseReports.SESSION_ID).toString() : null;
                //Check grading completion with latest session id
                if (latestSessionId != null) {
                    List<Map> inprogressListOfGradeStatus = Base.findAll(AJEntityBaseReports.FETCH_INPROGRESS_GRADE_STATUS_BY_SESSION_ID, userID, latestSessionId, collId);
                    if (inprogressListOfGradeStatus != null && !inprogressListOfGradeStatus.isEmpty()) gradeStatus = JsonConstants.IN_PROGRESS;
                }
                assData.put(AJEntityBaseReports.ATTR_GRADE_STATUS, gradeStatus);
                assessmentArray.add(assData);
              });
            }
            lessonData.put(JsonConstants.SOURCELIST, assessmentArray);
            UnitKpiArray.add(lessonData);

          }
        } else {
          LOGGER.info("No data returned for Student Perf in Assessment");
        }

      } else {
        LOGGER.info("Could not get Student Unit Performance");
      }

      contentBody.put(JsonConstants.USAGE_DATA, UnitKpiArray).put(JsonConstants.USERUID, userID);
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
