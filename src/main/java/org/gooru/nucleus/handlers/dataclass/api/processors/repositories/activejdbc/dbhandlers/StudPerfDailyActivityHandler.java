package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.utils.PgUtils;
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

public class StudPerfDailyActivityHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfDailyActivityHandler.class);

    private final ProcessorContext context;
    private String classId;
    private String userId;
    private String sDate;
    private String eDate;
    private JsonArray collectionIds;
    private String collectionType;

    public StudPerfDailyActivityHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        try {
            validateContextRequest();
            userId = this.context.request().getString(MessageConstants.USER_ID);
            sDate = this.context.request().getString(MessageConstants.START_DATE);
            eDate = this.context.request().getString(MessageConstants.END_DATE);
            collectionType = this.context.request().getString(MessageConstants.COLLECTION_TYPE);
            collectionIds = this.context.request().getJsonArray(MessageConstants.COLLECTION_IDS);
            classId = this.context.request().getString(MessageConstants.CLASS_ID);
            validateContextRequestFields();

        } catch (MessageResponseWrapperException mrwe) {
            return new ExecutionResult<>(mrwe.getMessageResponse(), ExecutionResult.ExecutionStatus.FAILED);
        }
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
        if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
            && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
            List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(),
                this.context.userIdFromSession());
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
    @SuppressWarnings({ "rawtypes" })
    public ExecutionResult<MessageResponse> executeRequest() {
        JsonObject resultBody = new JsonObject();
        JsonArray userUsageArray = new JsonArray();
        LOGGER.debug("userId : {} - collectionIds:{}", userId, collectionIds);
        Date startDate = Date.valueOf(sDate);
        Date endDate = Date.valueOf(eDate);
        String collTypeFilter = AJEntityDailyClassActivity.COLL_TYPE_FILTER;
        if (!collectionType.equalsIgnoreCase(EventConstants.COLLECTION))
            collTypeFilter = AJEntityDailyClassActivity.ASMT_TYPE_FILTER;
        List<String> collIds = new ArrayList<>(collectionIds.size());
        for (Object collId : collectionIds) {
            collIds.add(collId.toString());
        }
        List<String> userIds = fetchUserId(collTypeFilter);
        
        for (String userId : userIds) {
            JsonArray activityArray = new JsonArray();
            JsonObject dateActivity = new JsonObject();

            List<Map> activityList = null;
            if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
                LOGGER.debug("Fetching Performance for Assessments in Class");
                activityList = Base.findAll(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS, classId,
                    PgUtils.listToPostgresArrayString(collIds), userId, AJEntityDailyClassActivity.ATTR_CP_EVENTNAME, startDate,
                    endDate);
            } else {
                LOGGER.debug("Fetching Performance for Collections in Class");
                activityList = Base.findAll(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_COLLECTIONS, classId,
                    PgUtils.listToPostgresArrayString(collIds), userId, startDate, endDate);
            }
            if (activityList != null && !activityList.isEmpty()) {
                generateActivityData(activityArray, activityList);
            } else {
                LOGGER.debug("No data available for ANY of the collectionIds passed on to this endpoint");
            }
            dateActivity.put(JsonConstants.ACTIVITY, activityArray);
            dateActivity.put(JsonConstants.USERID, userId);
            userUsageArray.add(dateActivity);
        }

        resultBody.put(JsonConstants.USAGE_DATA, userUsageArray);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

    }

    @SuppressWarnings("rawtypes")
    private void generateActivityData(JsonArray activityArray, List<Map> activityList) {
        activityList.forEach(m -> {
            JsonObject contentKpi = new JsonObject();
            contentKpi.put(AJEntityDailyClassActivity.DATE, m.get(AJEntityDailyClassActivity.ACTIVITY_DATE).toString());
            contentKpi.put(AJEntityDailyClassActivity.ATTR_COLLECTION_ID, m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString());
            contentKpi.put(AJEntityDailyClassActivity.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityDailyClassActivity.ATTR_TIME_SPENT).toString()));
            contentKpi.put(AJEntityDailyClassActivity.ATTR_ATTEMPTS, m.get(AJEntityDailyClassActivity.ATTR_ATTEMPTS) != null ? Integer.parseInt(m.get(AJEntityDailyClassActivity.ATTR_ATTEMPTS).toString()) : 1);
            contentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
            if (collectionType.equalsIgnoreCase(JsonConstants.ASSESSMENT)) {
                contentKpi.put(AJEntityDailyClassActivity.ATTR_SCORE, Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
                contentKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID, m.get(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID).toString());
            } else {
                calculateCollectionScore(userId, m, contentKpi);
                contentKpi.put(AJEntityDailyClassActivity.ATTR_LAST_SESSION_ID, AJEntityDailyClassActivity.NA);
            }
            activityArray.add(contentKpi);
        });
    }

    @SuppressWarnings("rawtypes")
    private void calculateCollectionScore(String userId, Map m, JsonObject collectionKpi) {
        double maxScore = 0;
        if (!StringUtil.isNullOrEmpty(classId)) {
            List<Map> collectionMaximumScore = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_MAX_SCORE,
                classId, m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString(), userId,
                Date.valueOf(m.get(AJEntityDailyClassActivity.ACTIVITY_DATE).toString()));
            for (Map ms : collectionMaximumScore) {
                if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
                    maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
                }
            }
        }

        double scoreInPercent = 0;
        Object collectionScore = Base.firstCell(AJEntityDailyClassActivity.GET_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE,
            classId, m.get(AJEntityDailyClassActivity.ATTR_COLLECTION_ID).toString(), userId,
            Date.valueOf(m.get(AJEntityDailyClassActivity.ACTIVITY_DATE).toString()));

        if (collectionScore != null && (maxScore > 0)) {
            scoreInPercent = ((Double.valueOf(collectionScore.toString()) / maxScore) * 100);
            collectionKpi.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
        } else {
            collectionKpi.putNull(AJEntityBaseReports.SCORE);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchUserId(String addCollTypeFilterToQuery) {
        List<String> userIds;
        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is not in the request to fetch Student Performance in Course. Assume user is a teacher");
            LazyList<AJEntityDailyClassActivity> userIdOfClass = AJEntityDailyClassActivity.findBySQL(
                AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_DAILY_CLASS_ACTIVITY + addCollTypeFilterToQuery,
                this.classId);
            userIds = userIdOfClass.collect(AJEntityDailyClassActivity.GOORUUID);
        } else {
            userIds = new ArrayList<>(1);
            userIds.add(userId);
        }
        return userIds;
    }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }

    private void validateContextRequest() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch Student Performance in Assessments");
            throw new MessageResponseWrapperException(MessageResponseFactory
                .createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessments"));
        }
    }

    private void validateContextRequestFields() {
        if (StringUtil.isNullOrEmpty(eDate) || StringUtil.isNullOrEmpty(sDate)) {
            LOGGER.warn("Start Date and End Date are mandatory to fetch Student Performance in Daily Class Activity.");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse(
                "Start Date and/or End Date is Missing. Cannot fetch Student Performance in Daily Class Activity"));
        }
        if (StringUtil.isNullOrEmpty(collectionType)) {
            LOGGER.warn("Collection Type is mandatory to fetch Student Performance in Daily Class Activity.");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse(
                "Collection Type is Missing. Cannot fetch Student Performance in Daily Class Activity"));
        }
        if (collectionIds.isEmpty()) {
            LOGGER.warn("CollectionIds are mandatory to fetch Student Performance in Assessments");
            throw new MessageResponseWrapperException(MessageResponseFactory
                .createNotFoundResponse("CollectionIds are Missing. Cannot fetch Student Performance for Assessments"));
        }
        if (StringUtil.isNullOrEmpty(classId)) {
            LOGGER.warn("ClassId is mandatory for fetching Student Performance in Daily Class Activity");
            throw new MessageResponseWrapperException(MessageResponseFactory
                .createNotFoundResponse("Class Id Missing. Cannot fetch Student Performance in Daily Class Activity"));
        }
    }
}
