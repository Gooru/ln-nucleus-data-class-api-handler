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
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollectionPerformance;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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

public class StudentCourseAllItemsPerformanceHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudentCourseAllItemsPerformanceHandler.class);

    private final ProcessorContext context;
    private String classId;
    private String courseId;
    private String userId;
    private String sDate;
    private Integer limit;
    private Integer offset;

    public StudentCourseAllItemsPerformanceHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        try {
            validateContextRequest();
            userId = this.context.request().getString(MessageConstants.USER_ID);
            sDate = this.context.request().getString(MessageConstants.START_DATE);
            classId = this.context.classId();
            courseId = this.context.request().getString(MessageConstants.COURSE_ID);
            limit = Integer.valueOf(this.context.request().getString(MessageConstants.LIMIT, "50"));
            offset = Integer.valueOf(this.context.request().getString(MessageConstants.OFFSET, "0"));
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
        if (this.context.classId() != null) {
            if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
                List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
                if (owner.isEmpty()) {
                    LOGGER.debug("validateRequest() FAILED");
                    return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
                }
            }
        }
        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {

        JsonObject resultBody = new JsonObject();
        JsonArray resultarray = new JsonArray();
        Date startDate = Date.valueOf(sDate);
        List<String> userIds = fetchUserId();
        LOGGER.debug("UID is " + userIds);

        for (String userID : userIds) {

            List<Map> assessmentKpi = Base.findAll(AJEntityCollectionPerformance.SELECT_ITEM_PERF_IN_CLASS, context.classId(), courseId, userID, startDate, offset, limit);
            if (!assessmentKpi.isEmpty()) {
                JsonObject contentBody = new JsonObject();
                JsonArray collectionKpiArray = new JsonArray();
                assessmentKpi.forEach(m -> {
                    JsonObject collectionKpi = new JsonObject();
                    String collectionType = m.get(AJEntityBaseReports.ATTR_COLLECTION_TYPE).toString();
                    Double score = m.get(AJEntityBaseReports.SCORE) != null ? Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString()) : null;
                    Double maxScore = m.get(AJEntityBaseReports.ATTR_MAX_SCORE) != null ? Double.valueOf(m.get(AJEntityBaseReports.ATTR_MAX_SCORE).toString()) : null;
                    if (collectionType.equalsIgnoreCase(JsonConstants.COLLECTION)) {
                        if ((maxScore != null && maxScore > 0) && score != null) {
                            collectionKpi.put(AJEntityBaseReports.ATTR_SCORE, score);
                        } else {
                            collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
                        }
                        collectionKpi.put(EventConstants.VIEWS, (m.get(AJEntityBaseReports.ATTR_ATTEMPTS) != null && Integer.valueOf(m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()) > 0) ? Integer.valueOf(m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()) : 1);
                    } else {
                        collectionKpi.put(AJEntityBaseReports.ATTR_SCORE,
                            m.get(AJEntityBaseReports.ATTR_SCORE) != null ? Math.round(Double.valueOf(m.get(AJEntityBaseReports.ATTR_SCORE).toString())) : null);
                        collectionKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS,
                            m.get(AJEntityBaseReports.ATTR_ATTEMPTS) != null ? Integer.valueOf(m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()) : null);
                    }

                    collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, collectionType);
                    String collId = m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString();
                    collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
                    Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
                    collectionKpi.put(JsonConstants.TITLE, (collTitle != null ? collTitle.toString() : "NA"));
                    collectionKpi.put(AJEntityBaseReports.ATTR_SESSION_ID, m.get(AJEntityBaseReports.ATTR_SESSION_ID).toString());
                    collectionKpi.put(AJEntityBaseReports.ATTR_CLASS_ID, context.classId());
                    collectionKpi.put(AJEntityBaseReports.ATTR_COURSE_ID, courseId);
                    collectionKpi.put(AJEntityBaseReports.ATTR_UNIT_ID, m.get(AJEntityBaseReports.ATTR_UNIT_ID) != null ? m.get(AJEntityBaseReports.ATTR_UNIT_ID).toString() : null);
                    collectionKpi.put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.ATTR_LESSON_ID) != null ? m.get(AJEntityBaseReports.ATTR_LESSON_ID).toString() : null);
                    collectionKpi.put(AJEntityBaseReports.ATTR_PATH_ID, (m.get(AJEntityBaseReports.ATTR_PATH_ID) != null && Integer.valueOf(m.get(AJEntityBaseReports.ATTR_PATH_ID).toString()) > 0) ? Integer.valueOf(m.get(AJEntityBaseReports.ATTR_PATH_ID).toString()) : null);
                    collectionKpi.put(AJEntityBaseReports.ATTR_PATH_TYPE, m.get(AJEntityBaseReports.ATTR_PATH_TYPE) != null ? m.get(AJEntityBaseReports.ATTR_PATH_TYPE).toString() : null);
                    collectionKpi.put(AJEntityBaseReports.ATTR_LAST_ACCESSED, m.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString());

                    collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT,
                        m.get(AJEntityBaseReports.ATTR_TIME_SPENT) != null ? Long.valueOf(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()) : 0);
                    collectionKpi.put(AJEntityBaseReports.ATTR_REACTION, m.get(AJEntityBaseReports.ATTR_REACTION) != null ? Integer.valueOf(m.get(AJEntityBaseReports.ATTR_REACTION).toString()) : 0);

                    collectionKpi.put(JsonConstants.STATUS, Boolean.valueOf(m.get(JsonConstants.STATUS).toString()) ? JsonConstants.COMPLETE : JsonConstants.IN_PROGRESS);

                    collectionKpi.put(AJEntityBaseReports.ATTR_GRADE_STATUS,
                       ( m.get(AJEntityBaseReports.IS_GRADED) != null && !Boolean.valueOf(m.get(AJEntityBaseReports.IS_GRADED).toString())) ? JsonConstants.IN_PROGRESS : JsonConstants.COMPLETE);
                    collectionKpiArray.add(collectionKpi);
                });
                               
                Object studClassStartDate = Base.firstCell(AJEntityCollectionPerformance.GET_STUDENT_CLASS_ACTIVITY_START_DATE,
                		context.classId(), courseId, userID);                
                String studentClassStartDate = studClassStartDate != null ? studClassStartDate.toString() : null; 
                contentBody.put(JsonConstants.USAGE_DATA, collectionKpiArray).put(JsonConstants.USERUID, userID).put(JsonConstants.START_DATE, studentClassStartDate);
                
                resultarray.add(contentBody);
            } else {
                LOGGER.debug("No data returned for Student All Items Perf in Class");
            }

        }
        resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);

        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

    private void validateContextRequest() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch items");
            throw new MessageResponseWrapperException(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch items"));
        }
    }

    private void validateContextRequestFields() {
        if (StringUtil.isNullOrEmpty(sDate)) {
            LOGGER.warn("Start Date is mandatory to fetch items.");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse("Start Date is Missing. Cannot fetch items"));
        }
        if (StringUtil.isNullOrEmpty(classId)) {
            LOGGER.warn("ClassId is mandatory for fetching items");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse("Class Id Missing. Cannot fetch items"));
        }
        if (StringUtil.isNullOrEmpty(courseId)) {
            LOGGER.warn("CourseId is mandatory for fetching items");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse("Course Id Missing. Cannot fetch items"));
        }
        if (limit < 0 || offset < 0) {
            LOGGER.warn("Limit/Offset requested is negative");
            throw new MessageResponseWrapperException(MessageResponseFactory.createNotFoundResponse("Limit/Offset requested should not be negative"));
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchUserId() {
        List<String> userIds;
        if (StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is not in the request to fetch Student All Items Perf in Class. Assume user is a teacher");
            LazyList<AJEntityCollectionPerformance> userIdOfClass =
                AJEntityCollectionPerformance.findBySQL(AJEntityCollectionPerformance.SELECT_DISTINCT_USERID_FOR_COURSE_ID, this.classId, this.courseId);
            userIds = userIdOfClass.collect(AJEntityDailyClassActivity.GOORUUID);
        } else {
            userIds = new ArrayList<>(1);
            userIds.add(userId);
        }
        return userIds;
    }

}
