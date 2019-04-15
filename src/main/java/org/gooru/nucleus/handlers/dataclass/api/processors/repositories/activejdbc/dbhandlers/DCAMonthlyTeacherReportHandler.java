package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCAMonthlyTeacherReportHandler implements DBHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DCAMonthlyTeacherReportHandler.class);

  private final ProcessorContext context;
  private static final String REQUEST_COLLECTION_TYPE = "collectionType";
  private static final String REQUEST_DIMENSION = "dimension";
  private static final String REQUEST_MONTH = "month";
  private static final String REQUEST_YEAR = "year";

  private static Integer year;

  public DCAMonthlyTeacherReportHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch DCA weekly/monhly report");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch DCA weekly/monhly report"), ExecutionStatus.FAILED);
    }
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");

    if (context.request().getString(REQUEST_DIMENSION) == null) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("dimension should be provided"),
          ExecutionStatus.FAILED);
    } else if (!context.request().getString(REQUEST_DIMENSION).equalsIgnoreCase("weekly")
        && !context.request().getString(REQUEST_DIMENSION).equalsIgnoreCase("monthly")) {
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Valid Dimensions are : weekly,monthly"), ExecutionStatus.FAILED);
    }

    if (context.request().getString(REQUEST_COLLECTION_TYPE) == null) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("collectionType should be provided!"),
          ExecutionStatus.FAILED);
    }
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    LOGGER.debug("DIMENSION : " + context.request().getString(REQUEST_DIMENSION));
    LOGGER.debug("MONTH : " + context.request().getString(REQUEST_MONTH));
    LOGGER.debug("YEAR : " + context.request().getString(REQUEST_YEAR));
    LOGGER.debug("collectionType : " + context.request().getString(REQUEST_COLLECTION_TYPE));
    LOGGER.debug("classId : " + context.classId());
    int currentYear = LocalDate.now().getYear();
    LOGGER.debug("currentYear : " + currentYear);
    String dimension = context.request().getString(REQUEST_DIMENSION);
    String month = context.request().getString(REQUEST_MONTH);
    year = context.request().getString(REQUEST_YEAR) != null
        ? Integer.parseInt(context.request().getString(REQUEST_YEAR))
        : 0;
    String collectionType = context.request().getString(REQUEST_COLLECTION_TYPE);
    if (year == null || year == 0) {
      year = currentYear;
    }
    StringBuilder userIdsQueryBuilder =
        new StringBuilder(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_DCA);
    if (dimension.equalsIgnoreCase("weekly")) {
      userIdsQueryBuilder.append(" AND to_char(updated_at,'Mon') = '" + month + "'");
    }

    JsonObject contentBody = new JsonObject();
    JsonArray studentArray = new JsonArray();

    LOGGER.debug("UserFindQuery  : {}", userIdsQueryBuilder.toString());
    LazyList<AJEntityBaseReports> userIdList = AJEntityDailyClassActivity
        .findBySQL(userIdsQueryBuilder.toString(), context.classId(), collectionType, year);
    userIdList.stream().forEach(users -> {
      JsonObject studentObject = new JsonObject();
      JsonArray monthlyArray = new JsonArray();
      studentObject.put(AJEntityBaseReports.ATTR_USER_ID,
          users.get(AJEntityDailyClassActivity.GOORUUID));

      if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
        // collectionType=assessment
        // Generate Aggregated Data Monthly wise...
        List<Map> monthlyAggData = null;
        if (dimension.equalsIgnoreCase("weekly")) {
          monthlyAggData =
              Base.findAll(AJEntityDailyClassActivity.DCA_WEEKLY_USAGE_ASSESSMENT_AGG_DATA,
                  context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year, month);
        } else {
          monthlyAggData =
              Base.findAll(AJEntityDailyClassActivity.DCA_MONTHLY_USAGE_ASSESSMENT_AGG_DATA,
                  context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year);
        }
        monthlyAggData.stream().forEach(monthAggData -> {
          JsonObject monthUsage = new JsonObject();
          monthUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, Math
              .round(Double.parseDouble(monthAggData.get(AJEntityBaseReports.SCORE).toString())));
          monthUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
              Long.parseLong(monthAggData.get(AJEntityBaseReports.TIME_SPENT).toString()));
          if (dimension.equalsIgnoreCase("weekly")) {
            monthUsage.put("week", monthAggData.get("week"));
          } else {
            monthUsage.put("month", monthAggData.get("month"));

          }
          // Generate Aggregated Data Assessment wise...
          List<Map> monthlyAssessmentData = null;
          if (dimension.equalsIgnoreCase("weekly")) {
            monthlyAssessmentData =
                Base.findAll(AJEntityDailyClassActivity.DCA_WEEKLY_USAGE_ASSESSMENT_DATA,
                    context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year, month,
                    monthAggData.get("week"));
          } else {
            monthlyAssessmentData = Base.findAll(
                AJEntityDailyClassActivity.DCA_MONTHLY_USAGE_ASSESSMENT_DATA, context.classId(),
                users.get(AJEntityDailyClassActivity.GOORUUID), year, monthAggData.get("month"));
          }
          JsonArray monthlyAssessmentArray = new JsonArray();
          monthlyAssessmentData.stream().forEach(monthAssessmentData -> {
            JsonObject assessmentUsage = new JsonObject();
            assessmentUsage.put(AJEntityDailyClassActivity.ATTR_SCORE,
                Math.round(Double.parseDouble(
                    monthAssessmentData.get(AJEntityDailyClassActivity.SCORE).toString())));
            assessmentUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(
                monthAssessmentData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
            assessmentUsage.put(AJEntityDailyClassActivity.ATTR_ASSESSMENT_ID,
                monthAssessmentData.get(AJEntityDailyClassActivity.COLLECTION_OID).toString());
            monthlyAssessmentArray.add(assessmentUsage);
          });
          monthUsage.put("assessments", monthlyAssessmentArray);
          monthlyArray.add(monthUsage);
        });

      } else {
        // collectionType=collection
        // Generate Aggregated Data Monthly wise...
        List<Map> monthlyAggData = null;
        if (dimension.equalsIgnoreCase("weekly")) {
          monthlyAggData =
              Base.findAll(AJEntityDailyClassActivity.DCA_WEEKLY_USAGE_COLLECTION_AGG_DATA,
                  context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year, month);
        } else {
          monthlyAggData =
              Base.findAll(AJEntityDailyClassActivity.DCA_MONTHLY_USAGE_COLLECTION_AGG_DATA,
                  context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year);
        }
        monthlyAggData.stream().forEach(monthAggData -> {
          JsonObject monthUsage = new JsonObject();
          double monthMaxScore =
              Double.valueOf(monthAggData.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
          if (monthMaxScore > 0 && (monthAggData.get(AJEntityDailyClassActivity.SCORE) != null)) {
            double sumOfScore =
                Double.valueOf(monthAggData.get(AJEntityDailyClassActivity.SCORE).toString());
            monthUsage.put(AJEntityDailyClassActivity.ATTR_SCORE,
                ((sumOfScore / monthMaxScore) * 100));
          } else {
            monthUsage.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
          }

          monthUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
              Long.parseLong(monthAggData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
          if (dimension.equalsIgnoreCase("weekly")) {
            monthUsage.put("month", monthAggData.get("month"));
          } else {
            monthUsage.put("week", monthAggData.get("week"));
          }
          // Generate Aggregated Data Collection wise...
          List<Map> monthlyCollectionData = null;
          if (dimension.equalsIgnoreCase("weekly")) {
            monthlyCollectionData =
                Base.findAll(AJEntityDailyClassActivity.DCA_WEEKLY_USAGE_COLLECTION_DATA,
                    context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year, month);
          } else {
            monthlyCollectionData =
                Base.findAll(AJEntityDailyClassActivity.DCA_MONTHLY_USAGE_COLLECTION_DATA,
                    context.classId(), users.get(AJEntityDailyClassActivity.GOORUUID), year, month,
                    monthAggData.get("week"));
          }
          JsonArray monthlyCollectionArray = new JsonArray();
          monthlyCollectionData.stream().forEach(monthCollectionData -> {
            JsonObject collectionUsage = new JsonObject();
            double maxScore = Double
                .valueOf(monthCollectionData.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
            if (maxScore > 0
                && (monthCollectionData.get(AJEntityDailyClassActivity.SCORE) != null)) {
              double sumOfScore = Double
                  .valueOf(monthCollectionData.get(AJEntityDailyClassActivity.SCORE).toString());
              LOGGER.debug("maxScore : {} , sumOfScore : {} ", maxScore, sumOfScore);
              collectionUsage.put(AJEntityDailyClassActivity.ATTR_SCORE,
                  ((sumOfScore / maxScore) * 100));
            } else {
              collectionUsage.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
            }
            collectionUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(
                monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
            collectionUsage.put(AJEntityDailyClassActivity.ATTR_ASSESSMENT_ID,
                monthCollectionData.get(AJEntityDailyClassActivity.COLLECTION_OID).toString());
            monthlyCollectionArray.add(collectionUsage);
          });
          monthUsage.put("collections", monthlyCollectionArray);
          monthlyArray.add(monthUsage);
        });

      }
      if (dimension.equalsIgnoreCase("weekly")) {
        studentObject.put("weekly", monthlyArray);
      } else {
        studentObject.put("monthly", monthlyArray);
      }
      studentArray.add(studentObject);
    });
    contentBody.put(JsonConstants.USAGE_DATA, studentArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody),
        ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}
