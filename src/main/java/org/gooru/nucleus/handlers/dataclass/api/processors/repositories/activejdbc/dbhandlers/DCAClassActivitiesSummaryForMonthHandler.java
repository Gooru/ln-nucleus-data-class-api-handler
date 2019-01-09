package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCAClassActivitiesSummaryForMonthHandler implements DBHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DCAClassActivitiesSummaryForMonthHandler.class);

    private final ProcessorContext context;
    private static final String FOR_MONTH = "for_month";
    private static final String FOR_YEAR = "for_year";
    private static Integer year;

    public DCAClassActivitiesSummaryForMonthHandler(ProcessorContext context) {
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
        
        try {
            if (context.request().getInteger(FOR_MONTH) == null || context.request().getInteger(FOR_MONTH) == 0) {
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Valid Month should be provided"), ExecutionStatus.FAILED);
            }
        } catch (ClassCastException e) {
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Month should be provided in Integer"), ExecutionStatus.FAILED);
        }

        LOGGER.debug("validateRequest() OK");

        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
        LOGGER.debug("MONTH : " + context.request().getInteger(FOR_MONTH));
        LOGGER.debug("YEAR : " + context.request().getInteger(FOR_YEAR));
        LOGGER.debug("classId : " + context.classId());
        int currentYear = LocalDate.now().getYear();
        LOGGER.debug("currentYear : " + currentYear);
        Integer month = context.request().getInteger(FOR_MONTH);
        year = context.request().getInteger(FOR_YEAR) != null ? context.request().getInteger(FOR_YEAR) : 0;
        if (year == null || year == 0) {
            year = currentYear;
        }

        JsonObject contentBody = new JsonObject();
        contentBody.put(AJEntityDailyClassActivity.ATTR_CLASS_ID, context.classId());
        JsonArray activitiesArray = new JsonArray();
        
        // Generate Aggregated Data Assessment wise...
        List<Map> monthlyAssessmentData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_ASMT_SUMMARY_FOR_MONTH, context.classId(), year, month);
        if (monthlyAssessmentData != null) {
            monthlyAssessmentData.forEach(monthAssessmentData -> {
                JsonObject assessmentUsage = new JsonObject();
                assessmentUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.parseDouble(monthAssessmentData.get(AJEntityDailyClassActivity.SCORE).toString())));
                assessmentUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(monthAssessmentData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
                assessmentUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, monthAssessmentData.get(AJEntityDailyClassActivity.COLLECTION_OID).toString());
                assessmentUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE, monthAssessmentData.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
                activitiesArray.add(assessmentUsage);
            });
        }

        // Generate Aggregated Data Collection wise...
        List<Map> monthlyCollectionData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_COLL_SUMMARY_FOR_MONTH, context.classId(), year, month);
        if (monthlyCollectionData != null) {
            monthlyCollectionData.forEach(monthCollectionData -> {
                JsonObject collectionUsage = new JsonObject();
                double maxScore = Double.valueOf(monthCollectionData.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
                if (maxScore > 0 && (monthCollectionData.get(AJEntityDailyClassActivity.SCORE) != null)) {
                    double sumOfScore = Double.valueOf(monthCollectionData.get(AJEntityDailyClassActivity.SCORE).toString());
                    LOGGER.debug("maxScore : {} , sumOfScore : {} ", maxScore, sumOfScore);
                    collectionUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, ((sumOfScore / maxScore) * 100));
                } else {
                    collectionUsage.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
                }
                collectionUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
                collectionUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, monthCollectionData.get(AJEntityDailyClassActivity.COLLECTION_OID).toString());
                collectionUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE, monthCollectionData.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
                activitiesArray.add(collectionUsage);
            });
        }
        contentBody.put(JsonConstants.USAGE_DATA, activitiesArray);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody), ExecutionStatus.SUCCESSFUL);
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
