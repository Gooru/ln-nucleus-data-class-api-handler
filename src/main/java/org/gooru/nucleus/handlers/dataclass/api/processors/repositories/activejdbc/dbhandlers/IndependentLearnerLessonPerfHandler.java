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

public class IndependentLearnerLessonPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndependentLearnerLessonPerfHandler.class);
  private static final String REQUEST_COLLECTION_TYPE = "collectionType";
  private final ProcessorContext context;
  private String collectionType;
  private String userId;
  private long questionCount;

  public IndependentLearnerLessonPerfHandler(ProcessorContext context) {
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

    this.userId = this.context.userIdFromSession();

    List<String> userIds = new ArrayList<>();

    userIds.add(this.userId);

    LOGGER.debug("UID is " + this.userId);

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      JsonArray LessonKpiArray = new JsonArray();
      LazyList<AJEntityBaseReports> collIDforlesson = null;

      collIDforlesson = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_DISTINCT_COLLID_FOR_LESSON_ID_FILTERBY_COLLTYPE,
              context.courseId(), context.unitId(), context.lessonId(), this.collectionType, userID);

      List<String> collIds = new ArrayList<>();
      if (!collIDforlesson.isEmpty()) {
        LOGGER.info("Got a list of Distinct collectionIDs for this lesson");
        collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
      }
      List<Map> assessmentKpi = null;
      if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {

        assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_LESSON_PERF_FOR_COLLECTION, context.courseId(), context.unitId(),
                context.lessonId(), listToPostgresArrayString(collIds), userID, EventConstants.COLLECTION_PLAY);
      } else {

        assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_LESSON_PERF_FOR_ASSESSMENT, context.courseId(), context.unitId(),
                context.lessonId(), listToPostgresArrayString(collIds), userID, EventConstants.COLLECTION_PLAY);

      }
      if (!assessmentKpi.isEmpty()) {
        assessmentKpi.forEach(m -> {
          JsonObject lessonKpi = ValueMapper.map(ResponseAttributeIdentifier.getLessonPerformanceAttributesMap(), m);
          // FIXME : revisit completed count and total count
          lessonKpi.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 1);
          lessonKpi.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
          lessonKpi.put(AJEntityBaseReports.ATTR_SCORE, Double.valueOf(m.get(AJEntityBaseReports.ATTR_SCORE).toString()));
          // FIXME: This logic to be revisited.
          if (this.collectionType.equalsIgnoreCase(JsonConstants.COLLECTION)) {
            List<Map> collectionQuestionCount = null;

            collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_COLLECTION_QUESTION_COUNT, context.courseId(), context.unitId(),
                    context.lessonId(), lessonKpi.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID), this.userId);

            collectionQuestionCount.forEach(qc -> {
              this.questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
            });
            double scoreInPercent = 0;
            if (this.questionCount > 0) {
              Object collectionScore = null;

              collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_COLLECTION_AGG_SCORE, context.courseId(), context.unitId(),
                      context.lessonId(), lessonKpi.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID), this.userId);

              if (collectionScore != null) {
                scoreInPercent = ((double) Double.valueOf(collectionScore.toString()) / this.questionCount) * 100;
              }
            }
            lessonKpi.put(AJEntityBaseReports.ATTR_SCORE, scoreInPercent);
            lessonKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, lessonKpi.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID));
            lessonKpi.remove(AJEntityBaseReports.ATTR_ASSESSMENT_ID);
            lessonKpi.put(EventConstants.VIEWS, lessonKpi.getInteger(EventConstants.ATTEMPTS));
            lessonKpi.remove(EventConstants.ATTEMPTS);
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
