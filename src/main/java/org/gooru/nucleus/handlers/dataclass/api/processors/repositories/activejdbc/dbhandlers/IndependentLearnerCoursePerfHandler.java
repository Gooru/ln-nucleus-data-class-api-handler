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
class IndependentLearnerCoursePerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndependentLearnerCoursePerfHandler.class);
  private static final String REQUEST_COLLECTION_TYPE = "collectionType";

  private final ProcessorContext context;
  private String collectionType;
  private String userId;
  // For stuffing Json
  private String unitId;

  public IndependentLearnerCoursePerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received to fetch Student Performance in Course");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in course"),
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

    this.userId = this.context.userIdFromSession();

    List<String> unitIds = new ArrayList<>();
    List<String> userIds = new ArrayList<>();
    userIds.add(this.userId);

    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      JsonArray CourseKpiArray = new JsonArray();

      LazyList<AJEntityBaseReports> unitIDforCourse = null;

      unitIDforCourse = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_DISTINCT_UNIT_ID_FOR_COURSE_ID_FILTERBY_COLLTYPE,
              context.courseId(), this.collectionType, userID);

      if (!unitIDforCourse.isEmpty()) {
        LOGGER.debug("Got a list of Distinct unitIDs for this Course");

        unitIDforCourse.forEach(unit -> unitIds.add(unit.getString(AJEntityBaseReports.UNIT_GOORU_OID)));
        List<Map> assessmentKpi = null;
        if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
          assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_COURSE_PERF_FOR_COLLECTION, context.courseId(), this.collectionType, userID,
                  listToPostgresArrayString(unitIds), EventConstants.COLLECTION_PLAY);
        } else {
          assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_INDEPENDENT_LEARNER_COURSE_PERF_FOR_ASSESSMENT, context.courseId(),
                  this.collectionType, userID, listToPostgresArrayString(unitIds), EventConstants.COLLECTION_PLAY);
        }
        if (!assessmentKpi.isEmpty()) {
          assessmentKpi.forEach(m -> {
            unitId = m.get(AJEntityBaseReports.UNIT_GOORU_OID).toString();
            LOGGER.debug("The Value of UNITID " + unitId);
            List<Map> completedCountMap = null;
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              // FIXME: Score will not be useful in CUL if collection so could
              // be incorrect from this below query.
              completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLL_COUNT_FOREACH_INDEPENDENT_LEARNER_UNIT_ID, context.courseId(), unitId,
                      this.collectionType, userID, EventConstants.COLLECTION_PLAY);

            } else {
              completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT_FOREACH_INDEPENDENT_LEARNER_UNIT_ID, context.courseId(), unitId,
                      this.collectionType, userID, EventConstants.COLLECTION_PLAY);
            }
            JsonObject unitData = ValueMapper.map(ResponseAttributeIdentifier.getCoursePerformanceAttributesMap(), m);
            completedCountMap.forEach(scoreCompletonMap -> {
              unitData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                      Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
              unitData.put(AJEntityBaseReports.ATTR_SCORE, Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_SCORE).toString()));
              LOGGER.debug("UnitID : {} - UserID : {} - Score : {}", unitId, userID,
                      Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_SCORE).toString()));
              LOGGER.debug("UnitID : {} - UserID : {} - completedCount : {}", unitId, userID,
                      Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));

            });
            // FIXME: Total count will be taken from nucleus core.
            unitData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
            // FIXME : Revisit this logic in future.
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              unitData.put(EventConstants.VIEWS, unitData.getInteger(EventConstants.ATTEMPTS));
              unitData.remove(EventConstants.ATTEMPTS);
            }
            CourseKpiArray.add(unitData);
          });
        } else {
          LOGGER.info("No data returned for Student Perf in Assessment");
          // return new
          // ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(),
          // ExecutionStatus.FAILED);
        }

      } else {
        LOGGER.info("Could not get Student Course Performance");
        // Return an empty resultBody instead of an Error
        // return new
        // ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(),
        // ExecutionStatus.FAILED);
      }

      // Form the required Json pass it on
      contentBody.put(JsonConstants.USAGE_DATA, CourseKpiArray).put(JsonConstants.USERUID, userID);
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
