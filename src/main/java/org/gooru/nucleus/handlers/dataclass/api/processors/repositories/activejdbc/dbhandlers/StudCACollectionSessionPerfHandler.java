package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 * 
 */
public class StudCACollectionSessionPerfHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudCACollectionSessionPerfHandler.class);
    private static final String REQUEST_USERID = "userId";
    private double maxScore = 0.0;
    private long lastAccessedTime;

    private final ProcessorContext context;
    private String userId;

    public StudCACollectionSessionPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch Student Collection Perf in req session");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Collection"), ExecutionStatus.FAILED);
        }

        userId = this.context.request().getString(REQUEST_USERID);
        if (userId == null || userId.trim().isEmpty()) {
            LOGGER.warn("User Id is mandatory to fetch Student Collection Perf in req session");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("user Id is missing"), ExecutionStatus.FAILED);
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

        List<Map> collectionMaximumScore = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_MAX_SCORE_FOR_SESSION, context.sessionId());
        collectionMaximumScore.forEach(ms -> {
            if (ms.get(AJEntityDailyClassActivity.MAX_SCORE) != null) {
                this.maxScore = Double.valueOf(ms.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
            } else {
                this.maxScore = 0.0;
            }
        });

        List<Map> lastAccessedTime = Base.findAll(AJEntityDailyClassActivity.SELECT_LAST_ACCESSED_TIME_OF_SESSION, context.sessionId());

        if (!lastAccessedTime.isEmpty()) {
            lastAccessedTime.forEach(l -> {
                this.lastAccessedTime = l.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP) != null ? Timestamp.valueOf(l.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString()).getTime() : null;
            });
        }

        List<Map> collectionData = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_DATA_FOR_SESSION, context.sessionId());
        JsonObject contentBody = new JsonObject();
        if (!collectionData.isEmpty()) {
            LOGGER.debug("Collection Attributes obtained");
            collectionData.forEach(m -> {
                JsonObject collectionDataObj = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCACollectionAttributesMap(), m);
                collectionDataObj.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
                // Update this to be COLLECTION_TYPE in response
                collectionDataObj.put(EventConstants.COLLECTION_TYPE, m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
                collectionDataObj.put(JsonConstants.SCORE, m.get(AJEntityDailyClassActivity.SCORE) != null ? Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString())) : null);
                collectionDataObj.put(JsonConstants.MAX_SCORE, this.maxScore);
                double scoreInPercent;
                int reaction = 0;
                Object collectionScore = Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_SCORE_FOR_SESSION, context.sessionId());

                if (collectionScore != null && (this.maxScore > 0)) {
                    scoreInPercent = ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
                    collectionDataObj.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent));
                } else {
                    collectionDataObj.putNull(AJEntityDailyClassActivity.SCORE);
                }

                Object collectionReaction = Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_REACTION_FOR_SESSION, context.sessionId());

                if (collectionReaction != null) {
                    reaction = Integer.valueOf(collectionReaction.toString());
                }
                collectionDataObj.put(AJEntityDailyClassActivity.ATTR_REACTION, (reaction));
                contentBody.put(JsonConstants.COLLECTION, collectionDataObj);
            });

            LOGGER.debug("Collection resource Attributes started");
            List<Map> collectionQuestionsKPI;

            collectionQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_DATA_FOR_SESSION, context.sessionId());
            JsonArray questionsArray = new JsonArray();

            if (!collectionQuestionsKPI.isEmpty()) {
                collectionQuestionsKPI.forEach(questions -> {
                    JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
                    if (questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null) {
                        qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()));
                    }
                    // Default answerStatus will be skipped
                    if (qnData.getString(EventConstants.RESOURCE_TYPE).equalsIgnoreCase(EventConstants.QUESTION)) {
                        qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
                    }
                    List<Map> questionScore =
                        Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_QUESTION_AGG_SCORE_FOR_SESSION, context.sessionId(), questions.get(AJEntityDailyClassActivity.RESOURCE_ID));

                    if (questionScore != null && !questionScore.isEmpty()) {
                        questionScore.forEach(qs -> {
                            qnData.put(JsonConstants.ANSWER_OBJECT,
                                qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null ? new JsonArray(qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()) : null);
                            // Rubrics - Score may be NULL only incase of OE questions
                            qnData.put(JsonConstants.RAW_SCORE,
                                qs.get(AJEntityDailyClassActivity.SCORE) != null ? Math.round(Double.valueOf(qs.get(AJEntityDailyClassActivity.SCORE).toString())) : "NA");
                            qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityDailyClassActivity.ATTR_ATTEMPT_STATUS).toString());
                            qnData.put(JsonConstants.MAX_SCORE, qs.get(AJEntityDailyClassActivity.MAX_SCORE) != null ? Double.valueOf(qs.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : "NA");
                            Double quesScore = qs.get(AJEntityDailyClassActivity.SCORE) != null ? Double.valueOf(qs.get(AJEntityDailyClassActivity.SCORE).toString()) : null;
                            Double maxScore = qs.get(AJEntityDailyClassActivity.MAX_SCORE) != null ? Double.valueOf(qs.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : 0.0;
                            double scoreInPercent;
                            if (quesScore != null && (maxScore != null && maxScore > 0)) {
                                scoreInPercent = ((quesScore / maxScore) * 100);
                                qnData.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent));
                            } else {
                                qnData.put(AJEntityDailyClassActivity.SCORE, "NA");
                            }
                        });
                    }
                    // Get grading status for Questions
                    if (qnData.getString(EventConstants.QUESTION_TYPE).equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)) {
                        Object isGradedObj = Base.firstCell(AJEntityDailyClassActivity.GET_OE_QUE_GRADE_STATUS_FOR_SESSION_ID, context.sessionId(),
                            questions.get(AJEntityDailyClassActivity.RESOURCE_ID));
                        if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t") || isGradedObj.toString().equalsIgnoreCase("true"))) {
                            qnData.put(JsonConstants.IS_GRADED, true);
                        } else {
                            qnData.put(JsonConstants.IS_GRADED, false);
                        }
                    } else {
                        qnData.put(JsonConstants.IS_GRADED, "NA");
                    }

                    List<Map> resourceReaction;
                    resourceReaction =
                        Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_REACTION_FOR_SESSION, context.sessionId(), questions.get(AJEntityDailyClassActivity.RESOURCE_ID));

                    if (!resourceReaction.isEmpty()) {
                        resourceReaction.forEach(rs -> {
                            qnData.put(JsonConstants.REACTION, Integer.valueOf(rs.get(AJEntityDailyClassActivity.REACTION).toString()));
                            LOGGER.debug("Resource reaction: {} - resourceId : {}", rs.get(AJEntityDailyClassActivity.REACTION).toString(), questions.get(AJEntityDailyClassActivity.RESOURCE_ID));
                        });
                    }
                    questionsArray.add(qnData);
                });
            }
            contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userId);
            resultarray.add(contentBody);

        } else {
            LOGGER.debug("No data returned for this Student for this Collection");
        }
        resultBody.put(JsonConstants.CONTENT, resultarray);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
    }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }

}
