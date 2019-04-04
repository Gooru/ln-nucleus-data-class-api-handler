package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
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

public class DCAClassActivitiesSummaryForMonthHandler implements DBHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DCAClassActivitiesSummaryForMonthHandler.class);

    private final ProcessorContext context;
    private String userId;
    private static final String FOR_MONTH = "forMonth";
    private static final String FOR_YEAR = "forYear";
    private static final String REQUEST_USERID = "userUid";
    private static Integer year;
    private static Integer month;

    public DCAClassActivitiesSummaryForMonthHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        try {
            if (context.request() == null || context.request().isEmpty()) {
                LOGGER.warn("Invalid request received to fetch Class Activities Summary report");
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Class Activities Summary report"), ExecutionStatus.FAILED);
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

    JsonObject contentBody = new JsonObject();
    JsonArray activitiesArray = new JsonArray();

    // Generate Aggregated Data Assessment wise...
    List<Map> monthlyAssessmentData = null;
    if (!StringUtil.isNullOrEmptyAfterTrim(this.userId)) {
      monthlyAssessmentData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_ASMT_SUMMARY_FOR_MONTH_FOR_USER, context.classId(), year, month, this.userId);
    } else {
      monthlyAssessmentData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_ASMT_SUMMARY_FOR_MONTH, context.classId(), year, month);
    }
    if (monthlyAssessmentData != null) {
        monthlyAssessmentData.forEach(monthAssessmentData -> {
            JsonObject assessmentUsage = new JsonObject();
            assessmentUsage.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.parseDouble(monthAssessmentData.get(AJEntityDailyClassActivity.SCORE).toString())));
            assessmentUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, monthAssessmentData.get(AJEntityDailyClassActivity.COLLECTION_OID).toString());
            assessmentUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE, monthAssessmentData.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
            activitiesArray.add(assessmentUsage);
        });
    }

    // Generate Aggregated Data Collection wise...
    List<Map> monthlyCollectionData = null;
    if (!StringUtil.isNullOrEmptyAfterTrim(this.userId)) {
      monthlyCollectionData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_COLL_SUMMARY_FOR_MONTH_FOR_USER, context.classId(), year, month, this.userId);
    } else {
      monthlyCollectionData = Base.findAll(AJEntityDailyClassActivity.DCA_CLASS_COLL_SUMMARY_FOR_MONTH, context.classId(), year, month);
    }
    if (monthlyCollectionData != null) {
        monthlyCollectionData.forEach(monthCollectionData -> {
            JsonObject collectionUsage = new JsonObject();
            collectionUsage.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(monthCollectionData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
            collectionUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, monthCollectionData.get(AJEntityDailyClassActivity.COLLECTION_OID).toString());
            collectionUsage.put(AJEntityDailyClassActivity.ATTR_COLLECTION_TYPE, monthCollectionData.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
            activitiesArray.add(collectionUsage);
        });
    }
  
    contentBody.put(JsonConstants.USAGE_DATA, activitiesArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(contentBody),
        ExecutionStatus.SUCCESSFUL);
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
        this.userId = this.context.request().getString(REQUEST_USERID);
        LOGGER.debug("MONTH : {}", month);
        LOGGER.debug("YEAR : {}", year);
        LOGGER.debug("userId : {} - classId:{}", this.userId, context.classId());
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
