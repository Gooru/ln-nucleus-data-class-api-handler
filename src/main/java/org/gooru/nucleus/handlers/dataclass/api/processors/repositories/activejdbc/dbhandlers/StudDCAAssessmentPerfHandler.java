package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudDCAAssessmentPerfHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudDCAAssessmentPerfHandler.class);
	private static final String REQUEST_USERID = "userId";
	private static final String DATE = "date";	
	
    private final ProcessorContext context;

    public StudDCAAssessmentPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Assessment");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessment"),
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
    
    String tDate = this.context.request().getString(DATE);

    if (StringUtil.isNullOrEmpty(tDate)) {
      LOGGER.warn("Date is mandatory to fetch Student Performance in Daily Class Activity.");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
              "Date is Missing. Cannot fetch Student Performance in Daily Class Activity"), ExecutionStatus.FAILED);

    }
    Date date = Date.valueOf(tDate);

    String userId = this.context.request().getString(REQUEST_USERID);

    List<String> userIds = new ArrayList<>();
    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is not in the request to fetch Student Performance in DCA Collection. Assume user is a teacher");
      LazyList<AJEntityDailyClassActivity> userIdforlesson =
              AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_ASSESSMENT 
            		  + AJEntityDailyClassActivity.ASMT_TYPE_FILTER, context.classId(),
                      context.collectionId(), date, 
                      AJEntityDailyClassActivity.ATTR_CP_EVENTNAME, AJEntityDailyClassActivity.ATTR_EVENTTYPE_STOP);
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityDailyClassActivity.GOORUUID)));
    } else {
      userIds.add(userId);
    }

    LOGGER.debug("UID is " + userId);
    for (String userID : userIds) {
      JsonObject contentBody = new JsonObject();
      
      AJEntityDailyClassActivity dcaAssessmentPerfModel =  AJEntityDailyClassActivity.findFirst("actor_id = ? AND class_id = ? AND collection_id = ? "
      		+ "AND date_in_time_zone = ? AND event_name = 'collection.play' AND event_type = 'stop' ORDER BY updated_at DESC", 
    		  userID, context.classId(),context.collectionId(), date);
      
		if (dcaAssessmentPerfModel != null) {
			String studentLatestSessionId = dcaAssessmentPerfModel.getString(AJEntityDailyClassActivity.SESSION_ID); 
			Object assessmentReactionObject =  Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_REACTION, context.collectionId(), 
					studentLatestSessionId, date);
			
			LOGGER.debug("Assessment Attributes obtained");
			JsonObject assessmentData = new JsonObject();
			assessmentData.put(JsonConstants.SCORE, dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.SCORE) != null 
					? Math.round(Double.valueOf(dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.SCORE).toString())) : null);
			assessmentData.put(JsonConstants.REACTION, assessmentReactionObject != null ? ((Number)assessmentReactionObject).intValue() : 0);
			assessmentData.put(JsonConstants.TIMESPENT, dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.TIMESPENT) != null 
					? dcaAssessmentPerfModel.getLong(AJEntityDailyClassActivity.TIMESPENT) : 0);
			assessmentData.put(JsonConstants.COLLECTION_TYPE, dcaAssessmentPerfModel.get(AJEntityDailyClassActivity.COLLECTION_TYPE) != null
					? dcaAssessmentPerfModel.getString(AJEntityDailyClassActivity.COLLECTION_TYPE) : null);				
			contentBody.put(JsonConstants.ASSESSMENT, assessmentData);    				
					
			
			List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,context.collectionId(),
					studentLatestSessionId, date, AJEntityDailyClassActivity.ATTR_CRP_EVENTNAME);
			LOGGER.debug("latestSessionId : " + studentLatestSessionId);
			
			JsonArray questionsArray = new JsonArray();
			if (!assessmentQuestionsKPI.isEmpty()) {
				for (Map questions : assessmentQuestionsKPI) {
					JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), questions);
					qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
					qnData.put(JsonConstants.ANSWER_OBJECT, questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null
							? new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()) : null);
					//Rubrics - Score should be NULL only incase of OE questions
					qnData.put(JsonConstants.RAW_SCORE, questions.get(AJEntityDailyClassActivity.SCORE) != null ?
							Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString())) : "NA");
					Object reactionObj = Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_RESOURCE_REACTION, context.collectionId(),
							studentLatestSessionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString());
					qnData.put(JsonConstants.REACTION, reactionObj != null ? ((Number)reactionObj).intValue() : 0);
					qnData.put(JsonConstants.MAX_SCORE, questions.get(AJEntityDailyClassActivity.MAX_SCORE) != null ?
							Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.MAX_SCORE).toString())) : "NA");
					Double quesScore = questions.get(AJEntityBaseReports.SCORE) != null ? Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString()) : null;
					Double maxScore = questions.get(AJEntityBaseReports.MAX_SCORE) != null ? Double.valueOf(questions.get(AJEntityBaseReports.MAX_SCORE).toString()) : 0.0;
					double scoreInPercent;
					if (quesScore != null && (maxScore != null && maxScore > 0)) {
					    scoreInPercent = ((quesScore / maxScore) * 100);
					    qnData.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
					} else {
					    qnData.put(AJEntityBaseReports.SCORE, "NA");
					}
					if(qnData.getString(EventConstants.QUESTION_TYPE).equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)){
						Object isGradedObj = Base.firstCell(AJEntityDailyClassActivity.GET_OE_QUE_GRADE_STATUS, context.collectionId(),
								studentLatestSessionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString(), date);
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
			contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userID);
			resultarray.add(contentBody);

      } else {
        LOGGER.debug("No data returned for Student Perf in Assessment");
      }
    }
    resultBody.put(JsonConstants.CONTENT, resultarray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
  }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }


}
