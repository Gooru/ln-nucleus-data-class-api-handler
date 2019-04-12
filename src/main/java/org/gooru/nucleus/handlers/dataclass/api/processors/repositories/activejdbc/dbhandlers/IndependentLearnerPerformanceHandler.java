package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityILBookmarkContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
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

public class IndependentLearnerPerformanceHandler implements DBHandler {


  private static final Logger LOGGER =
      LoggerFactory.getLogger(IndependentLearnerPerformanceHandler.class);

  private final ProcessorContext context;
  private static final String REQUEST_USERID = "userId";
  private static final String REQUEST_CONTENTTYPE = "contentType";
  private static final int MAX_LIMIT = 20;
  int questionCount;

  IndependentLearnerPerformanceHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch Independent Learner's Location");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Invalid data provided to fetch Student Performance in Assessments"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
        && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
      LOGGER.debug("validateRequest() FAILED");
      return new ExecutionResult<>(
          MessageResponseFactory.createForbiddenResponse("User validation failed"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    String userId = this.context.request().getString(REQUEST_USERID);
    if (StringUtil.isNullOrEmpty(userId)) {
      LOGGER.error("userId is Mandatory to fetch Independent Learner's performance");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "userId Missing. Cannot fetch Independent Learner's Performance"),
          ExecutionStatus.FAILED);
    }


    String contentType = this.context.request().getString(REQUEST_CONTENTTYPE);

    if (StringUtil.isNullOrEmpty(contentType)) {
      LOGGER.error("contentType is Mandatory to fetch Independent Learner's performance");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "contentType Missing. Cannot fetch Independent Learner's Performance"),
          ExecutionStatus.FAILED);
    }


    String limitS = this.context.request().getString("limit");
    String offsetS = this.context.request().getString("offset");

    JsonObject result = new JsonObject();
    JsonArray ILPerfArray = new JsonArray();

    if (!StringUtil.isNullOrEmpty(contentType)
        && contentType.equalsIgnoreCase(MessageConstants.COURSE)) {
      // @NILE-1329
      List<String> courseIds = new ArrayList<>();
      LazyList<AJEntityILBookmarkContent> ILCourses = AJEntityILBookmarkContent.findBySQL(
          AJEntityILBookmarkContent.SELECT_DISTINCT_IL_CONTENTID, userId,
          AJEntityILBookmarkContent.ATTR_COURSE);
      if (!ILCourses.isEmpty()) {
        ILCourses.forEach(
            course -> courseIds.add(course.get(AJEntityILBookmarkContent.CONTENT_ID).toString()));
        for (String c : courseIds) {
          LOGGER.debug("Course Ids are" + c);
        }
        List<String> cIds = new ArrayList<>();

        LazyList<AJEntityBaseReports> ILLatestCourses = AJEntityBaseReports.findBySQL(
            AJEntityBaseReports.GET_LATEST_IL_COURSES, userId, listToPostgresArrayString(courseIds),
            (StringUtil.isNullOrEmpty(limitS) || (Integer.valueOf(limitS) > MAX_LIMIT)) ? MAX_LIMIT
                : Integer.valueOf(limitS),
            StringUtil.isNullOrEmpty(offsetS) ? 0 : Integer.valueOf(offsetS));

        if (!ILLatestCourses.isEmpty()) {
          ILLatestCourses.forEach(
              course -> cIds.add(course.get(AJEntityBaseReports.COURSE_GOORU_OID).toString()));
          for (String c : cIds) {
            LOGGER.debug("Latest Course Ids are" + c);
          }
          List<Map> courseTSKpi = Base.findAll(AJEntityBaseReports.GET_IL_ALL_COURSE_TIMESPENT,
              listToPostgresArrayString(cIds), userId);

          if (!courseTSKpi.isEmpty()) {
            courseTSKpi.forEach(courseTS -> {
              JsonObject courseDataObject = new JsonObject();
              courseDataObject.put(AJEntityBaseReports.ATTR_COURSE_ID,
                  courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
              Object title = Base.firstCell(AJEntityContent.GET_TITLE,
                  courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
              courseDataObject.put(JsonConstants.COURSE_TITLE,
                  (title != null ? title.toString() : "NA"));
              courseDataObject.put(AJEntityBaseReports.ATTR_TIME_SPENT,
                  Long.parseLong(courseTS.get(AJEntityBaseReports.TIME_SPENT).toString()));
              List<Map> courseCompletionKpi =
                  Base.findAll(AJEntityBaseReports.GET_IL_ALL_COURSE_SCORE_COMPLETION, userId,
                      courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
              courseCompletionKpi.forEach(courseComplettion -> {
                courseDataObject.put(AJEntityBaseReports.ATTR_SCORE,
                    courseComplettion.get(AJEntityBaseReports.ATTR_SCORE) != null
                        ? Math.round(Double.valueOf(
                            courseComplettion.get(AJEntityBaseReports.ATTR_SCORE).toString()))
                        : null);
                courseDataObject.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, Integer.parseInt(
                    courseComplettion.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
              });
              Object courseTotalAssCount =
                  Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT,
                      courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
              courseDataObject.put(AJEntityBaseReports.ATTR_TOTAL_COUNT,
                  courseTotalAssCount != null ? Integer.valueOf(courseTotalAssCount.toString())
                      : 0);
              ILPerfArray.add(courseDataObject);
            });
          }
        }
      } else {
        LOGGER.info("No data returned for Independent Learner for All Courses");
      }

    } else if (!StringUtil.isNullOrEmpty(contentType)
        && contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {

      // @NILE-1329
      List<String> assessmentIds = new ArrayList<>();
      LazyList<AJEntityILBookmarkContent> ILAssessments = AJEntityILBookmarkContent.findBySQL(
          AJEntityILBookmarkContent.SELECT_DISTINCT_IL_CONTENTID, userId,
          AJEntityILBookmarkContent.ATTR_ASSESSMENT);
      if (!ILAssessments.isEmpty()) {
        ILAssessments.forEach(
            a -> assessmentIds.add(a.get(AJEntityILBookmarkContent.CONTENT_ID).toString()));
        for (String c : assessmentIds) {
          LOGGER.debug("Assessment Ids are" + c);
        }
        List<String> aIds = new ArrayList<>();

        LazyList<AJEntityBaseReports> ILLatestAssessments = AJEntityBaseReports.findBySQL(
            AJEntityBaseReports.GET_LATEST_IL_ASSESSMENTS, userId,
            listToPostgresArrayString(assessmentIds),
            (StringUtil.isNullOrEmpty(limitS) || (Integer.valueOf(limitS) > MAX_LIMIT)) ? MAX_LIMIT
                : Integer.valueOf(limitS),
            StringUtil.isNullOrEmpty(offsetS) ? 0 : Integer.valueOf(offsetS));

        if (!ILLatestAssessments.isEmpty()) {
          ILLatestAssessments
              .forEach(as -> aIds.add(as.get(AJEntityBaseReports.COLLECTION_OID).toString()));
          for (String a : aIds) {
            LOGGER.debug("Latest Assessment Ids are" + a);
          }

          List<Map> assessmentTS =
              Base.findAll(AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_ATTEMPTS_TIMESPENT,
                  listToPostgresArrayString(aIds), userId);

          if (!assessmentTS.isEmpty()) {
            assessmentTS.forEach(assessmentTsKpi -> {
              JsonObject assesmentObject = new JsonObject();
              assesmentObject.put(AJEntityBaseReports.ATTR_COLLECTION_ID,
                  assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
              assesmentObject.put(AJEntityBaseReports.ATTR_TIME_SPENT,
                  Long.parseLong(assessmentTsKpi.get(AJEntityBaseReports.TIME_SPENT).toString()));
              assesmentObject.put(AJEntityBaseReports.ATTR_ATTEMPTS, Long
                  .parseLong(assessmentTsKpi.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()));
              List<Map> assessmentCompletionKpi =
                  Base.findAll(AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_SCORE_COMPLETION, userId,
                      assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
              if (!assessmentCompletionKpi.isEmpty()) {
                assessmentCompletionKpi.forEach(
                    courseComplettion -> assesmentObject.put(AJEntityBaseReports.ATTR_SCORE,
                        courseComplettion.get(AJEntityBaseReports.ATTR_SCORE) != null
                            ? Math.round(Double.valueOf(
                                courseComplettion.get(AJEntityBaseReports.ATTR_SCORE).toString()))
                            : null));
              } else {
                assesmentObject.putNull(AJEntityBaseReports.ATTR_SCORE);
              }
              Object title = Base.firstCell(AJEntityContent.GET_TITLE,
                  assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
              assesmentObject.put(JsonConstants.COLLECTION_TITLE,
                  (title != null ? title.toString() : "NA"));
              ILPerfArray.add(assesmentObject);
            });
          }
        }
      } else {
        LOGGER.info("No data returned for Independent Learner for Standalone Assessments");
      }


    } else if (!StringUtil.isNullOrEmpty(contentType)
        && contentType.equalsIgnoreCase(MessageConstants.COLLECTION)) {

      // @NILE-1329
      List<String> collectionIds = new ArrayList<>();
      LazyList<AJEntityILBookmarkContent> ILCollections = AJEntityILBookmarkContent.findBySQL(
          AJEntityILBookmarkContent.SELECT_DISTINCT_IL_CONTENTID, userId,
          AJEntityILBookmarkContent.ATTR_COLLECTION);
      if (!ILCollections.isEmpty()) {
        ILCollections.forEach(
            a -> collectionIds.add(a.get(AJEntityILBookmarkContent.CONTENT_ID).toString()));
        for (String c : collectionIds) {
          LOGGER.info("Collection Ids are" + c);
        }

        List<String> collIds = new ArrayList<>();

        LazyList<AJEntityBaseReports> ILLatestCollections = AJEntityBaseReports.findBySQL(
            AJEntityBaseReports.GET_LATEST_IL_COLLECTIONS, userId,
            listToPostgresArrayString(collectionIds),
            (StringUtil.isNullOrEmpty(limitS) || (Integer.valueOf(limitS) > MAX_LIMIT)) ? MAX_LIMIT
                : Integer.valueOf(limitS),
            StringUtil.isNullOrEmpty(offsetS) ? 0 : Integer.valueOf(offsetS));

        if (!ILLatestCollections.isEmpty()) {

          ILLatestCollections.forEach(
              coll -> collIds.add(coll.get(AJEntityBaseReports.COLLECTION_OID).toString()));
          for (String co : collIds) {
            LOGGER.debug("Latest Collection Ids are" + co);
          }

          List<Map> collectionTS =
              Base.findAll(AJEntityBaseReports.GET_IL_ALL_COLLECTION_VIEWS_TIMESPENT,
                  listToPostgresArrayString(collIds), userId);

          if (!collectionTS.isEmpty()) {
            collectionTS.forEach(collectionsKpi -> {
              JsonObject collectionObj = new JsonObject();
              String collId = collectionsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString();
              collectionObj.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
              collectionObj.put(AJEntityBaseReports.ATTR_TIME_SPENT,
                  Long.parseLong(collectionsKpi.get(AJEntityBaseReports.TIME_SPENT).toString()));
              collectionObj.put(AJEntityBaseReports.VIEWS,
                  Long.parseLong(collectionsKpi.get(AJEntityBaseReports.VIEWS).toString()));

              List<Map> collectionScore =
                  Base.findAll(AJEntityBaseReports.GET_IL_ALL_COLLECTION_SCORE, collId, userId);
              collectionScore.forEach(score -> {
                double maxScore =
                    Double.valueOf(score.get(AJEntityBaseReports.MAX_SCORE).toString());
                if (maxScore > 0 && (score.get(AJEntityBaseReports.SCORE) != null)) {
                  double sumOfScore =
                      Double.valueOf(score.get(AJEntityBaseReports.SCORE).toString());
                  LOGGER.debug("maxScore : {} , sumOfScore : {} ", maxScore, sumOfScore);
                  collectionObj.put(AJEntityBaseReports.ATTR_SCORE,
                      ((sumOfScore / maxScore) * 100));
                } else {
                  collectionObj.putNull(AJEntityBaseReports.ATTR_SCORE);
                }
              });

              Object title = Base.firstCell(AJEntityContent.GET_TITLE,
                  collectionsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
              collectionObj.put(JsonConstants.COLLECTION_TITLE,
                  (title != null ? title.toString() : "NA"));
              ILPerfArray.add(collectionObj);
            });
          }
        }
      } else {
        LOGGER.info("No data returned for Independent Learner for Standalone Collections");
      }
    }

    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, ILPerfArray).put(JsonConstants.USERID, userId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
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
