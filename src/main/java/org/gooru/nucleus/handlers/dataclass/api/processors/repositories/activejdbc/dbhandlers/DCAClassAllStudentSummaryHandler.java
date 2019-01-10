package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCAClassAllStudentSummaryHandler implements DBHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DCAClassAllStudentSummaryHandler.class);

    private final ProcessorContext context;
    private static final String FOR_MONTH = "forMonth";
    private static final String FOR_YEAR = "forYear";

    private static Integer year;
    private static Integer month;

    public DCAClassAllStudentSummaryHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        try {
            if (context.request() == null || context.request().isEmpty()) {
                LOGGER.warn("Invalid request received to fetch DCA Class All Students Summary report");
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch DCA Class All Students Summary report"), ExecutionStatus.FAILED);
            }

            if (context.request().getString(FOR_YEAR) == null) {
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Year should be provided"), ExecutionStatus.FAILED);
            }

            if (context.request().getString(FOR_MONTH) == null) {
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Month should be provided"), ExecutionStatus.FAILED);
            }
            validateRequestParamData();
        } catch (MessageResponseWrapperException mrwe) {
            return new ExecutionResult<>(mrwe.getMessageResponse(), ExecutionResult.ExecutionStatus.FAILED);
        }
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }    

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
        if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
            List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
            if (owner.isEmpty()) {
                LOGGER.debug("validateRequest() FAILED");
                return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
            }
        }
        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
        LOGGER.debug("MONTH : {}", month);
        LOGGER.debug("YEAR : {}", year);
        LOGGER.debug("collectionType : {}", context.collectionType());
        LOGGER.debug("classId : {}", context.classId());
        String collectionType = context.collectionType();
        
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
                collectionUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
                user.put(AJEntityBaseReports.ATTR_USER_ID, monthCollectionData.get(AJEntityDailyClassActivity.GOORUUID).toString());
                user.put(JsonConstants.USAGE_DATA, collectionUsage);
                studentArray.add(user);
            });

        }
        contentBody.put(JsonConstants.USERS, studentArray);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody), ExecutionStatus.SUCCESSFUL);
    }
    
    private void validateRequestParamData() {
        try {
            year = Integer.parseInt(context.request().getString(FOR_YEAR));
            month = Integer.parseInt(context.request().getString(FOR_MONTH));
        } catch (Exception e) {
            throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse("Format of Year/ Month is invalid!"));
        }
        if (!AJEntityDailyClassActivity.YEAR_PATTERN.matcher(context.request().getString(FOR_YEAR)).matches() || (month < 1 || month > 12)) {
            throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse("Year/ Month is invalid!"));
        }        
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
