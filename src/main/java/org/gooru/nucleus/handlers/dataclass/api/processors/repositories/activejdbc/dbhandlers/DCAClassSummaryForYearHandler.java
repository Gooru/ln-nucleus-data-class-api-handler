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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public class DCAClassSummaryForYearHandler implements DBHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DCAClassSummaryForYearHandler.class);

    private final ProcessorContext context;
    private static final String FOR_YEAR = "for_year";
    private static final String MONTH = "month";

    private static Integer year;

    public DCAClassSummaryForYearHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch DCA monhly report");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch DCA weekly/monhly report"), ExecutionStatus.FAILED);
        }
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {

        try {
            if (context.request().getInteger(FOR_YEAR) == null) {
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Valid Year should be provided"), ExecutionStatus.FAILED);
            }
        } catch (ClassCastException e) {
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Year should be provided in Integer"), ExecutionStatus.FAILED);
        }

        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
        LOGGER.debug("YEAR : " + context.request().getInteger(FOR_YEAR));
        LOGGER.debug("classId : " + context.classId());
        int currentYear = LocalDate.now().getYear();
        LOGGER.debug("currentYear : " + currentYear);
        year = context.request().getInteger(FOR_YEAR) != null ? context.request().getInteger(FOR_YEAR) : 0;
        if (year == null || year == 0) {
            year = currentYear;
        }

        JsonObject contentBody = new JsonObject();
        contentBody.put(AJEntityBaseReports.ATTR_CLASS_ID, context.classId());

        JsonObject monthlyObject = new JsonObject();
        // Generate Aggregated Data Month wise...
        List<Map> monthlyAggData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_SCORE_MONTHLY_SUMMARY, context.classId(), year);
        if (monthlyAggData != null) {
            monthlyAggData.forEach(monthAggData -> {
                JsonObject monthUsage = new JsonObject();
                monthUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.parseDouble(monthAggData.get(AJEntityBaseReports.SCORE).toString())));
                List<Map> monthlyCollectionData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_TS_SUMMARY_FOR_MONTH, context.classId(), year, monthAggData.get("monthAsInt"));
                if (monthlyCollectionData != null) {
                    monthlyCollectionData.forEach(monthCollectionData -> {
                        monthUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
                            monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT) != null ? Long.parseLong(monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()) : 0l);
                    });
                }
                monthlyObject.put(monthAggData.get(MONTH).toString().toLowerCase(), monthUsage);
            });
        }
        contentBody.put(JsonConstants.USAGE_DATA, monthlyObject);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody), ExecutionStatus.SUCCESSFUL);
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
