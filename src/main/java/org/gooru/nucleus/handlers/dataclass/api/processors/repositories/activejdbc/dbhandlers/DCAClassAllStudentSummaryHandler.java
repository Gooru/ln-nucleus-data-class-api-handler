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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCAClassAllStudentSummaryHandler implements DBHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DCAClassAllStudentSummaryHandler.class);

    private final ProcessorContext context;
    private static final String FOR_MONTH = "for_month";
    private static final String FOR_YEAR = "for_year";

    private static Integer year;

    public DCAClassAllStudentSummaryHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch DCA weekly/monhly report");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch DCA weekly/monhly report"), ExecutionStatus.FAILED);
        }
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
        LOGGER.debug("validateRequest() OK");

        if (context.request().getString(FOR_MONTH) == null) {
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("month should be provided"), ExecutionStatus.FAILED);
        }
        if (context.request().getString(FOR_YEAR) == null) {
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("year should be provided"), ExecutionStatus.FAILED);
        }
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
        LOGGER.debug("MONTH : " + context.request().getString(FOR_MONTH));
        LOGGER.debug("YEAR : " + context.request().getString(FOR_YEAR));
        LOGGER.debug("collectionType : " + context.collectionType());
        LOGGER.debug("classId : " + context.classId());
        int currentYear = LocalDate.now().getYear();
        LOGGER.debug("currentYear : " + currentYear);
        String month = context.request().getString(FOR_MONTH);
        year = context.request().getString(FOR_YEAR) != null ? Integer.parseInt(context.request().getString(FOR_YEAR)) : 0;
        String collectionType = context.collectionType();
        if (year == null || year == 0) {
            year = currentYear;
        }

        JsonObject contentBody = new JsonObject();
        JsonArray studentArray = new JsonArray();

        if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
            // collectionType=assessment
            // Generate Aggregated Assessment Data for Month
            List<Map> monthlyAssessmentData = null;
            monthlyAssessmentData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_USER_USAGE_ASSESSMENT_DATA, context.classId(), context.collectionId(), year, month);
            monthlyAssessmentData.stream().forEach(monthAssessmentData -> {
                JsonObject assessmentUsage = new JsonObject();
                JsonObject user = new JsonObject();
                assessmentUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.parseDouble(monthAssessmentData.get(AJEntityDailyClassActivity.SCORE).toString())));
                assessmentUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(monthAssessmentData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
                user.put(AJEntityBaseReports.ATTR_USER_ID, monthAssessmentData.get(AJEntityDailyClassActivity.GOORUUID).toString());
                user.put(JsonConstants.USAGE_DATA, assessmentUsage);
                studentArray.add(user);
            });

        } else {
            // collectionType=collection
            // Generate Aggregated Collection Data for Month
            List<Map> monthlyCollectionData = null;
            monthlyCollectionData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_USER_USAGE_COLLECTION_DATA, context.classId(), context.collectionId(), year, month);
            monthlyCollectionData.stream().forEach(monthCollectionData -> {
                JsonObject collectionUsage = new JsonObject();
                JsonObject user = new JsonObject();
                double maxScore = Double.valueOf(monthCollectionData.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
                if (maxScore > 0 && (monthCollectionData.get(AJEntityDailyClassActivity.SCORE) != null)) {
                    double sumOfScore = Double.valueOf(monthCollectionData.get(AJEntityDailyClassActivity.SCORE).toString());
                    LOGGER.debug("maxScore : {} , sumOfScore : {} ", maxScore, sumOfScore);
                    collectionUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, ((sumOfScore / maxScore) * 100));
                } else {
                    collectionUsage.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
                }
                collectionUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
                user.put(AJEntityBaseReports.ATTR_USER_ID, monthCollectionData.get(AJEntityDailyClassActivity.GOORUUID).toString());
                user.put(JsonConstants.USAGE_DATA, collectionUsage);
                studentArray.add(user);
            });

        }
        contentBody.put(JsonConstants.USERS, studentArray);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody), ExecutionStatus.SUCCESSFUL);
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
