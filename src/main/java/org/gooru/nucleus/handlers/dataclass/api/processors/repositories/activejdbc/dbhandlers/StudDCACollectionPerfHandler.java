package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
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

public class StudDCACollectionPerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(StudDCACollectionPerfHandler.class);
	private static final String REQUEST_USERID = "userId";
	private static final String DATE = "date";
	private double maxScore = 0.0;
	private long lastAccessedTime;	 
	private String sessionId;


	private final ProcessorContext context;
	public StudDCACollectionPerfHandler(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public ExecutionResult<MessageResponse> checkSanity() {
		if (context.request() == null || context.request().isEmpty()) {
			LOGGER.warn("Invalid request received to fetch Student Performance in Collection");
			return new ExecutionResult<>(
					MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Collection"),
					ExecutionStatus.FAILED);
		}

		LOGGER.debug("checkSanity() OK");
		return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public ExecutionResult<MessageResponse> validateRequest() {
		      if (context.getUserIdFromRequest() == null
		              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
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
		
		String classId = context.request().getString(MessageConstants.CLASS_ID);
		// For DCA activities, the summary report should be fetched based only on
		// classId and collectionId. (CourseId, UnitId and lessonId are not expected)    
		String collectionId = context.collectionId();

		// For DCA activities, the summary report should be fetched based only on classId and collectionId. (CourseId, UnitId and lessonId are not expected)
		if (StringUtil.isNullOrEmpty(classId)) {
			LOGGER.warn("ClassId is mandatory to fetch Student Performance in a DCA Collection");
			return new ExecutionResult<>(
					MessageResponseFactory.createInvalidRequestResponse("ClassId Missing. Cannot fetch Collection Summary in DCA"),
					ExecutionStatus.FAILED);
		}

		String tDate = this.context.request().getString(DATE);    

		if (StringUtil.isNullOrEmpty(tDate)) {
			LOGGER.warn("Date is mandatory to fetch Student Collection Performance in Daily Class Activity.");
			return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
					"Date is Missing. Cannot fetch Student Collection Performance in Daily Class Activity"), ExecutionStatus.FAILED);

		}
		Date date = Date.valueOf(tDate);    
		String userId = this.context.request().getString(REQUEST_USERID);

		List<String> userIds = new ArrayList<>();
		if (context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
			LOGGER.warn("UserID is not in the request to fetch Student Performance in DCA Collection. Assume user is a teacher");
			LazyList<AJEntityDailyClassActivity> userIdforCollection =
					AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_COLLECTION_ID + AJEntityDailyClassActivity.COLL_TYPE_FILTER, context.classId(),
							context.collectionId(), date);      
			userIdforCollection.forEach(coll -> userIds.add(coll.getString(AJEntityDailyClassActivity.GOORUUID)));      
		} else {
			userIds.add(userId);
		}

		for (String userID : userIds) {    	
			if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(collectionId)) {
				List<Map> collectionMaximumScore = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_MAX_SCORE,
						classId, collectionId, userID, date);
				collectionMaximumScore.forEach(ms -> {
					if (ms.get(AJEntityDailyClassActivity.MAX_SCORE) != null) {
						this.maxScore = Double.valueOf(ms.get(AJEntityDailyClassActivity.MAX_SCORE).toString());
					} else {
						this.maxScore = 0.0;
					}
				});
			} else {
				this.maxScore = 0.0;
			}

			List<Map> lastAccessedTime = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_LAST_ACCESSED_TIME,
					classId, collectionId, userID, date);

			if (!lastAccessedTime.isEmpty()) {
				lastAccessedTime.forEach(l -> {
					this.lastAccessedTime = l.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP) != null
							? Timestamp.valueOf(l.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString()).getTime()
									: null;
							this.sessionId = l.get(AJEntityDailyClassActivity.SESSION_ID) != null
									? l.get(AJEntityDailyClassActivity.SESSION_ID).toString() : "NA";
				});
			}

			List<Map> collectionData = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_DATA, classId,
					collectionId, userID, date);
			JsonObject contentBody = new JsonObject();
			if (!collectionData.isEmpty()) {
				LOGGER.debug("Collection Attributes obtained");
				collectionData.forEach(m -> {
					JsonObject collectionDataObj = ValueMapper
							.map(ResponseAttributeIdentifier.getSessionDCACollectionAttributesMap(), m);
					collectionDataObj.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
					collectionDataObj.put(EventConstants.SESSION_ID, this.sessionId);
					// Update this to be COLLECTION_TYPE in response
					collectionDataObj.put(EventConstants.COLLECTION_TYPE,
							m.get(AJEntityDailyClassActivity.COLLECTION_TYPE).toString());
					collectionDataObj.put(JsonConstants.SCORE, m.get(AJEntityDailyClassActivity.SCORE) != null
							? Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString())) : null);
					collectionDataObj.put(JsonConstants.MAX_SCORE, this.maxScore);

					double scoreInPercent;
					int reaction = 0;
					Object collectionScore = Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_SCORE,
							classId, collectionId, userID, date);

					if (collectionScore != null && (this.maxScore > 0)) {
						scoreInPercent = ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
						collectionDataObj.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent));
					} else {
						collectionDataObj.putNull(AJEntityDailyClassActivity.SCORE);
					}

					Object collectionReaction;
					collectionReaction = Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_REACTION,
							classId, collectionId, userID, date);

					if (collectionReaction != null) {
						reaction = Integer.valueOf(collectionReaction.toString());
					}
					collectionDataObj.put(AJEntityDailyClassActivity.ATTR_REACTION, (reaction));
					contentBody.put(JsonConstants.COLLECTION, collectionDataObj);
				});		

				LOGGER.debug("Collection resource Attributes started");
				List<Map> collectionQuestionsKPI;

				collectionQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_DATA, classId, collectionId, userID, date);
				JsonArray questionsArray = new JsonArray();

				if(!collectionQuestionsKPI.isEmpty()){
					collectionQuestionsKPI.forEach(questions -> {
						JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
						if(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null){
							qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()));
						}
						//Default answerStatus will be skipped
						if(qnData.getString(EventConstants.RESOURCE_TYPE).equalsIgnoreCase(EventConstants.QUESTION)){
							qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
						}
						List<Map> questionScore = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_QUESTION_AGG_SCORE, classId, collectionId, 
								questions.get(AJEntityDailyClassActivity.RESOURCE_ID), userID, date);

						String latestSessionId = null;
						if(questionScore != null && !questionScore.isEmpty()){
							latestSessionId = (questionScore.get(0).get(AJEntityDailyClassActivity.SESSION_ID) != null) ? 
									questionScore.get(0).get(AJEntityDailyClassActivity.SESSION_ID).toString() : null;
									questionScore.forEach(qs -> {
										qnData.put(JsonConstants.ANSWER_OBJECT, qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null
												? new JsonArray(qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()) : null);
										//Rubrics - Score may be NULL only incase of OE questions
										qnData.put(JsonConstants.SCORE, qs.get(AJEntityDailyClassActivity.SCORE) != null ?
												Double.valueOf(qs.get(AJEntityDailyClassActivity.SCORE).toString()) : "NA");
										qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityDailyClassActivity.ATTR_ATTEMPT_STATUS).toString());
										qnData.put(JsonConstants.MAX_SCORE, qs.get(AJEntityDailyClassActivity.MAX_SCORE) != null ?
												Double.valueOf(qs.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : "NA");
									});
						}
						//Get grading status for Questions
						if (latestSessionId != null && qnData.getString(EventConstants.QUESTION_TYPE).equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)){
							Object isGradedObj = Base.firstCell(AJEntityDailyClassActivity.GET_OE_QUE_GRADE_STATUS, collectionId, latestSessionId, 
									questions.get(AJEntityDailyClassActivity.RESOURCE_ID), date);
							if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t") || isGradedObj.toString().equalsIgnoreCase("true"))) {
								qnData.put(JsonConstants.IS_GRADED, true);
							} else {
								qnData.put(JsonConstants.IS_GRADED, false);
							}
						} else {
							qnData.put(JsonConstants.IS_GRADED, "NA");
						}

						List<Map> resourceReaction;
						resourceReaction = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_REACTION, classId, collectionId,
								questions.get(AJEntityDailyClassActivity.RESOURCE_ID), userID, date);

						if(!resourceReaction.isEmpty()){
							resourceReaction.forEach(rs ->{
								qnData.put(JsonConstants.REACTION, Integer.valueOf(rs.get(AJEntityDailyClassActivity.REACTION).toString()));
								LOGGER.debug("Resource reaction: {} - resourceId : {}" ,rs.get(AJEntityDailyClassActivity.REACTION).toString(), questions.get(AJEntityDailyClassActivity.RESOURCE_ID));
							});
						}
						qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
						questionsArray.add(qnData);     
					});    
				} 
				contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userID);
				resultarray.add(contentBody);         

			}else {
				LOGGER.debug("No data returned for this Student for this Collection");
			}
		} //loop per user
		resultBody.put(JsonConstants.CONTENT, resultarray);
		return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
	}

	@Override
	public boolean handlerReadOnly() {
		return true;
	}

}
