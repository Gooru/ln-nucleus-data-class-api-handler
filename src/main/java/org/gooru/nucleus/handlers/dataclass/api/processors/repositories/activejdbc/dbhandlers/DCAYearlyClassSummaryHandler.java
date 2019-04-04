package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCAYearlyClassSummaryHandler implements DBHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DCAYearlyClassSummaryHandler.class);

  private final ProcessorContext context;
  private String userId;
  private static final String YEAR = "year";
  private static final String MONTH = "month";
  private static final String REQUEST_USERID = "userUid";

  public DCAYearlyClassSummaryHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid request received to fetch DCA Class Summary yearly report");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Invalid data provided to fetch DCA weekly/monhly report"), ExecutionStatus.FAILED);
    }
    this.userId = this.context.request().getString(REQUEST_USERID);
    LOGGER.debug("userId : {} - classId:{}", this.userId, context.classId());
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

  }

  @SuppressWarnings("rawtypes")
  @Override
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
  public ExecutionResult<MessageResponse> executeRequest() {

    JsonObject contentBody = new JsonObject();
    JsonArray responseArray = new JsonArray();

    if (!StringUtil.isNullOrEmptyAfterTrim(this.userId)) {
      getStudentReport(responseArray);
    } else {
      getTeacherReport(responseArray);
    }
    contentBody.put(JsonConstants.USAGE_DATA, responseArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody),
        ExecutionStatus.SUCCESSFUL);
  }

  @SuppressWarnings("rawtypes")
  private void getTeacherReport(JsonArray responseArray) {
    // Generate Aggregated Data Month wise...
    List<Map> monthlyAggData = Base.findAll(
        AJEntityDailyClassActivity.DCA_CLASS_SCORE_YEAR_MONTH_BREAKDOWN, context.classId());
    if (monthlyAggData != null) {
      monthlyAggData.forEach(monthAggData -> {
        JsonObject monthlyUsage = new JsonObject();
        monthlyUsage.put(AJEntityDailyClassActivity.ATTR_SCORE,
            Math.round(Double.parseDouble(monthAggData.get(AJEntityBaseReports.SCORE).toString())));
        List<Map> monthlyCollectionData =
            Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_TS_SUMMARY_FOR_MONTH,
                context.classId(), monthAggData.get(YEAR), monthAggData.get(MONTH));
        if (monthlyCollectionData != null) {
          monthlyCollectionData.forEach(monthCollectionData -> {
            monthlyUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
                monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT) != null
                    ? Long.parseLong(
                        monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString())
                    : 0l);
          });
        }
        monthlyUsage.put(MONTH, Math.round((Double) monthAggData.get(MONTH)));
        monthlyUsage.put(YEAR, Math.round((Double) monthAggData.get(YEAR)));
        responseArray.add(monthlyUsage);
      });
    }
  }

  @SuppressWarnings("rawtypes")
  private void getStudentReport(JsonArray responseArray) {
    // Generate Aggregated Data Month wise...
    List<Map> monthlyAggData =
        Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_SCORE_YEAR_MONTH_BREAKDOWN_FOR_USER,
            context.classId(), this.userId);
    if (monthlyAggData != null) {
      monthlyAggData.forEach(monthAggData -> {
        JsonObject monthlyUsage = new JsonObject();
        monthlyUsage.put(AJEntityDailyClassActivity.ATTR_SCORE,
            Math.round(Double.parseDouble(monthAggData.get(AJEntityBaseReports.SCORE).toString())));
        List<Map> monthlyCollectionData =
            Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_TS_SUMMARY_FOR_MONTH_FOR_USER,
                context.classId(), monthAggData.get(YEAR), monthAggData.get(MONTH), this.userId);
        if (monthlyCollectionData != null) {
          monthlyCollectionData.forEach(monthCollectionData -> {
            monthlyUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
                monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT) != null
                    ? Long.parseLong(
                        monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString())
                    : 0l);
          });
        }
        monthlyUsage.put(MONTH, Math.round((Double) monthAggData.get(MONTH)));
        monthlyUsage.put(YEAR, Math.round((Double) monthAggData.get(YEAR)));
        responseArray.add(monthlyUsage);
      });
    }
  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}
