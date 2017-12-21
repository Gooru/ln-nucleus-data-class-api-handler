package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;

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
	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";

	//private static final String REQUEST_USERID = "userUid";
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
//      if (context.getUserIdFromRequest() == null
//              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
//        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
//        if (owner.isEmpty()) {
//            LOGGER.debug("validateRequest() FAILED");
//            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
//        }
//      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject resultBody = new JsonObject();
    JsonArray resultarray = new JsonArray();
    JsonObject assessmentDataKPI = new JsonObject();

    
    String sDate = this.context.request().getString(START_DATE);
    String eDate = this.context.request().getString(END_DATE);

    if (StringUtil.isNullOrEmpty(eDate) || StringUtil.isNullOrEmpty(sDate)) {
      LOGGER.warn("Start Date and End Date are mandatory to fetch Student Performance in Daily Class Activity.");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
              "Start Date and End Date are Missing. Cannot fetch Student Performance in Daily Class Activity"), ExecutionStatus.FAILED);

    }
    Date startDate = Date.valueOf(sDate);
    Date endDate = Date.valueOf(eDate);

    String collectionType = "assessment";
    String userId = this.context.request().getString(REQUEST_USERID);

    List<String> userIds = new ArrayList<>();
    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
      LOGGER.warn("UserID is not in the request to fetch Student Performance in DCA Collection. Assume user is a teacher");
      LazyList<AJEntityDailyClassActivity> userIdforlesson =
              AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.SELECT_DISTINCT_USERID_FOR_ASSESSMENT_ID_FILTERBY_COLLTYPE, context.classId(),
                      context.collectionId(), collectionType, startDate, endDate);
      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityDailyClassActivity.GOORUUID)));
    } else {
      userIds.add(userId);
    }

    LOGGER.debug("UID is " + userId);
    for (String userID : userIds) {
      List<Map> studentLatestAttempt;
      JsonArray contentArray = new JsonArray();
      studentLatestAttempt = Base.findAll(AJEntityDailyClassActivity.GET_LATEST_COMPLETED_SESSION_ID, context.classId(), 
    		  context.collectionId(), userID, startDate, endDate);

      if (!studentLatestAttempt.isEmpty()) {
        JsonObject contentBody = new JsonObject();
        studentLatestAttempt.forEach(attempts -> {
        	String sessionId = attempts.get(AJEntityDailyClassActivity.SESSION_ID).toString();
	        List<Map> assessmentKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID, context.collectionId(), 
	        		sessionId , userID, startDate, AJEntityDailyClassActivity.ATTR_CP_EVENTNAME);
	        Object assessmentReactionObject =  Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_REACTION_AND_SESSION_ID, 
	        		context.collectionId(), sessionId, userID);
	        
        	if (!assessmentKPI.isEmpty()) {
		          LOGGER.debug("Assessment Attributes obtained");
		          assessmentKPI.forEach(m -> {
		            JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCAAssessmentAttributesMap(), m);
		            assessmentData.put(JsonConstants.SCORE, m.get(AJEntityDailyClassActivity.SCORE) != null ? 
		            		Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString())) : null);
		            assessmentData.put(JsonConstants.REACTION, assessmentReactionObject != null ? ((Number)assessmentReactionObject).intValue() : 0);	            
		            assessmentDataKPI.put(JsonConstants.ASSESSMENT, assessmentData);
		          });

		          LOGGER.debug("Assessment question Attributes started");
      	
        List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,context.collectionId(),
                attempts.get(AJEntityDailyClassActivity.SESSION_ID).toString(), AJEntityDailyClassActivity.ATTR_CRP_EVENTNAME);
        LOGGER.debug("latestSessionId : " + attempts.get(AJEntityDailyClassActivity.SESSION_ID));
        JsonArray questionsArray = new JsonArray();
        if (!assessmentQuestionsKPI.isEmpty()) {
          assessmentQuestionsKPI.forEach(questions -> {
            JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCAAssessmentQuestionAttributesMap(), questions);
            //String sessionId = attempts.get(AJEntityDailyClassActivity.SESSION_ID).toString(); 
            qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
            //qnData.put(JsonConstants.SESSIONID, attempts.get(AJEntityDailyClassActivity.SESSION_ID).toString());
            Object reactionObj = Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_RESOURCE_REACTION, context.collectionId(),
          		  sessionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString());
            qnData.put(JsonConstants.REACTION, reactionObj != null ? ((Number)reactionObj).intValue() : 0);
            //Rubrics - Score should be NULL only incase of OE questions
            qnData.put(JsonConstants.SCORE, questions.get(AJEntityDailyClassActivity.SCORE) != null ?
          		  Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString())) : "NA");              
            questionsArray.add(qnData);
          });
        }        
        assessmentDataKPI.put(JsonConstants.QUESTIONS, questionsArray);
        LOGGER.debug("Assessment question Attributes obtained");
        contentArray.add(assessmentDataKPI);
        contentBody.put(JsonConstants.USAGE_DATA, contentArray).put(JsonConstants.USERUID, userID);        	
        } // AssessmentKPI End
        }); //Before this
        resultarray.add(contentBody);
      } else {
        // Return an empty resultBody instead of an Error
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
