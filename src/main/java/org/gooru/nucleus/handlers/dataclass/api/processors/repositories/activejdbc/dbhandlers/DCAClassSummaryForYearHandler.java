package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
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
    private static final String YEAR = "year";
    private static final String MONTH = "month";

    private static Integer limit;
    private static Integer offset;

    public DCAClassSummaryForYearHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch DCA monhly report");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch DCA weekly/monhly report"), ExecutionStatus.FAILED);
        }

        limit = Integer.valueOf(this.context.request().getString(MessageConstants.LIMIT, "20"));
        offset = Integer.valueOf(this.context.request().getString(MessageConstants.OFFSET, "0"));
        if (limit < 0 || offset < 0) {
            LOGGER.warn("Limit/Offset requested is negative");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse("Limit/Offset requested should not be negative"));
        }
        
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
        LOGGER.debug("classId : {}", context.classId());

        JsonObject contentBody = new JsonObject();
        JsonObject yearObject = new JsonObject();

        // Generate Aggregated Data Month wise...
        List<Map> monthlyAggData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_SCORE_MONTHLY_SUMMARY, context.classId(), limit, offset);
        if (monthlyAggData != null) {
            monthlyAggData.forEach(monthAggData -> {
                JsonObject monthlyObject = new JsonObject();
                JsonObject monthUsage = new JsonObject();
                monthUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.parseDouble(monthAggData.get(AJEntityBaseReports.SCORE).toString())));
                List<Map> monthlyCollectionData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_TS_SUMMARY_FOR_MONTH, context.classId(), monthAggData.get(YEAR), monthAggData.get(MONTH));
                if (monthlyCollectionData != null) {
                    monthlyCollectionData.forEach(monthCollectionData -> {
                        monthUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT,
                            monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT) != null ? Long.parseLong(monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()) : 0l);
                    });
                }
                Long month = Math.round(Double.parseDouble(monthAggData.get(MONTH).toString()));
                Long year = Math.round(Double.parseDouble(monthAggData.get(YEAR).toString()));
                monthlyObject.put(month.toString(), monthUsage);
                if (yearObject.containsKey(year.toString())) {
                    yearObject.getJsonObject(year.toString()).put(month.toString(), monthUsage);
                } else {
                    yearObject.put(year.toString(), monthlyObject);
                }
            });
        }
        contentBody.put(JsonConstants.USAGE_DATA, yearObject);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody), ExecutionStatus.SUCCESSFUL);
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
