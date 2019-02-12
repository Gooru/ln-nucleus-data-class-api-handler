package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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

/**
 * @author renuka
 * 
 */
public class StudCAAssessmentSessionPerfHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudCAAssessmentSessionPerfHandler.class);
    private static final String REQUEST_USERID = "userId";

    private final ProcessorContext context;

    public StudCAAssessmentSessionPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch Student Performance in session at CA");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessment"), ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
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

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
        JsonObject resultBody = new JsonObject();
        JsonArray resultarray = new JsonArray();

        String userId = this.context.request().getString(REQUEST_USERID);

        if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
            LOGGER.warn("UserID is not in the request to fetch Asmt Perf in session at CA. Assume user is a teacher");
            LazyList<AJEntityDailyClassActivity> userIdforAssessment =
                AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_SESSION + AJEntityDailyClassActivity.ASMT_TYPE_FILTER, context.classId(),
                    context.sessionId(), AJEntityDailyClassActivity.ATTR_CP_EVENTNAME, AJEntityDailyClassActivity.ATTR_EVENTTYPE_STOP);
            if (userIdforAssessment != null && !userIdforAssessment.isEmpty()) {
                AJEntityDailyClassActivity userIdC = userIdforAssessment.get(0);
                userId = userIdC.getString(AJEntityDailyClassActivity.GOORUUID);
            }
        }

        LOGGER.debug("UID is " + userId);
        JsonObject contentBody = new JsonObject();

        LazyList<AJEntityDailyClassActivity> dcaAssessmentPerf = AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.SELECT_ASSESSMENT_PERF_FOR_SESSION_ID, context.sessionId(),
            AJEntityDailyClassActivity.ATTR_CP_EVENTNAME, AJEntityDailyClassActivity.ATTR_EVENTTYPE_STOP);
        if (dcaAssessmentPerf != null && !dcaAssessmentPerf.isEmpty()) {
            AJEntityDailyClassActivity dcaAssessmentPerfModel = dcaAssessmentPerf.get(0);
            Object assessmentReactionObject = Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_REACTION_FOR_SESSION_ID, context.sessionId());

            LOGGER.debug("Assessment Attributes obtained");
            JsonObject assessmentData = new JsonObject();
            assessmentData.put(JsonConstants.SCORE,
                dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.SCORE) != null ? Math.round(Double.valueOf(dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.SCORE).toString())) : null);
            assessmentData.put(JsonConstants.REACTION, assessmentReactionObject != null ? ((Number) assessmentReactionObject).intValue() : 0);
            assessmentData.put(JsonConstants.TIMESPENT,
                dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.TIMESPENT) != null ? dcaAssessmentPerfModel.getLong(AJEntityDailyClassActivity.TIMESPENT) : 0);
            assessmentData.put(JsonConstants.COLLECTION_TYPE,
                dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.COLLECTION_TYPE) != null ? dcaAssessmentPerfModel.getString(AJEntityDailyClassActivity.COLLECTION_TYPE) : null);
            contentBody.put(JsonConstants.ASSESSMENT, assessmentData);

            List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_QUESTION_FOR_SESSION_ID, context.sessionId(), AJEntityDailyClassActivity.ATTR_CRP_EVENTNAME);

            JsonArray questionsArray = new JsonArray();
            if (!assessmentQuestionsKPI.isEmpty()) {
                for (Map questions : assessmentQuestionsKPI) {
                    JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), questions);
                    qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
                    qnData.put(JsonConstants.ANSWER_OBJECT,
                        questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null ? new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()) : null);
                    // Rubrics - Score should be NULL only incase of OE
                    // questions
                    qnData.put(JsonConstants.RAW_SCORE,
                        questions.get(AJEntityDailyClassActivity.SCORE) != null ? Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString())) : "NA");
                    Object reactionObj = Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_RESOURCE_REACTION_FOR_SESSION_ID, context.sessionId(),
                        questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString());
                    qnData.put(JsonConstants.REACTION, reactionObj != null ? ((Number) reactionObj).intValue() : 0);
                    qnData.put(JsonConstants.MAX_SCORE,
                        questions.get(AJEntityDailyClassActivity.MAX_SCORE) != null ? Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.MAX_SCORE).toString())) : "NA");
                    Double quesScore = questions.get(AJEntityDailyClassActivity.SCORE) != null ? Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString()) : null;
                    Double maxScore = questions.get(AJEntityDailyClassActivity.MAX_SCORE) != null ? Double.valueOf(questions.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : 0.0;
                    double scoreInPercent;
                    if (quesScore != null && (maxScore != null && maxScore > 0)) {
                        scoreInPercent = ((quesScore / maxScore) * 100);
                        qnData.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent));
                    } else {
                        qnData.put(AJEntityDailyClassActivity.SCORE, "NA");
                    }
                    if (qnData.getString(EventConstants.QUESTION_TYPE).equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)) {
                        Object isGradedObj =
                            Base.firstCell(AJEntityDailyClassActivity.GET_OE_QUE_GRADE_STATUS_FOR_SESSION_ID, context.sessionId(), questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString());
                        if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t") || isGradedObj.toString().equalsIgnoreCase("true"))) {
                            qnData.put(JsonConstants.IS_GRADED, true);
                        } else {
                            qnData.put(JsonConstants.IS_GRADED, false);
                        }
                    } else {
                        qnData.put(JsonConstants.IS_GRADED, true);
                    }
                    questionsArray.add(qnData);
                }
            }
            contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userId);
            resultarray.add(contentBody);

        } else {
            LOGGER.debug("No data returned for Asmt Perf in session at CA");
        }
        resultBody.put(JsonConstants.CONTENT, resultarray);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
    }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }

}
