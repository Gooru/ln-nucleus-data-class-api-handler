package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
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
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudDCAAssessmentSummaryHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudDCAAssessmentSummaryHandler.class);

	    private static final String REQUEST_SESSION_ID = "sessionId";
	    private final ProcessorContext context;
		private static final String DATE = "date";
		

	private String sessionId;
	    public StudDCAAssessmentSummaryHandler(ProcessorContext context) {
	        this.context = context;
	    }

	    @Override
	    public ExecutionResult<MessageResponse> checkSanity() {
	        if (context.request() == null || context.request().isEmpty()) {
	            LOGGER.warn("invalid request received to fetch Student Performance in Assessments");
	            return new ExecutionResult<>(
	                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Assessments"),
	                ExecutionStatus.FAILED);
	        }

	        LOGGER.debug("checkSanity() OK");
	        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	    }

	    @Override
	    @SuppressWarnings("rawtypes")
	    public ExecutionResult<MessageResponse> validateRequest() {
	        if (context.getUserIdFromRequest() == null
	                || (!context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
	          LOGGER.debug("Request by Teacher/collaborator....");
	          this.sessionId = this.context.request().getString(REQUEST_SESSION_ID);	          
	          Object classID = Base.firstCell(AJEntityDailyClassActivity.SELECT_CLASS_BY_SESSION_ID,context.collectionId(), sessionId);

	          if (classID == null) {
	            LOGGER.error("validateRequest() FAILED, No Class Association found.");
	            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("No Class Associated. DCA data can't be fetched by teacher/collaborator"),
	                    ExecutionStatus.FAILED);
	          } else {
	            List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,classID, this.context.userIdFromSession());
	            if (owner.isEmpty()) {
	              LOGGER.error("validateRequest() FAILED, User is not a Teacher");
	              return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
	            }
	          }
	        }
	        LOGGER.debug("validateRequest() OK");
	        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	    }

	    @Override
	    @SuppressWarnings("rawtypes")
	    public ExecutionResult<MessageResponse> executeRequest() {
	      JsonObject resultBody = new JsonObject();
	      JsonObject assessmentDataKPI = new JsonObject();
		  String userId = context.getUserIdFromRequest();
	      
	      this.sessionId = this.context.request().getString(REQUEST_SESSION_ID);
	      JsonArray contentArray = new JsonArray();
	      
	      String aDate = this.context.request().getString(DATE);

	      if (StringUtil.isNullOrEmpty(aDate)) {
	        LOGGER.warn("Date is mandatory to fetch Student Performance in Daily Class Activity.");
	        return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
	                "Date is Missing. Cannot fetch Student Performance in Daily Class Activity"), ExecutionStatus.FAILED);

	      }
	      Date assessmentDate = Date.valueOf(aDate);
	      LOGGER.debug("The assessment Id " + assessmentDate);
	      
	      if (!StringUtil.isNullOrEmpty(sessionId)) {
	        List<Map> assessmentKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID, context.collectionId(), 
	        		sessionId , userId, assessmentDate, AJEntityDailyClassActivity.ATTR_CP_EVENTNAME);
	        Object assessmentReactionObject =  Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_REACTION_AND_SESSION_ID, 
	        		context.collectionId(), sessionId, userId);

	        LOGGER.info("cID : {} , SID : {} ", context.collectionId(), sessionId);
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

	          List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID_FOR_SUMMARY,context.collectionId(),
	                  sessionId, AJEntityDailyClassActivity.ATTR_CRP_EVENTNAME);

	          JsonArray questionsArray = new JsonArray();
	          if(!assessmentQuestionsKPI.isEmpty()){
              assessmentQuestionsKPI.forEach(questions -> {
                JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCAAssessmentQuestionAttributesMap(), questions);
                Object reactionObj = Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_RESOURCE_REACTION, context.collectionId(), sessionId,
                        questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString());                
                qnData.put(JsonConstants.REACTION, reactionObj != null ? ((Number)reactionObj).intValue() : 0);
                
                if(qnData.getString(EventConstants.QUESTION_TYPE).equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)){
                    Object isGradedObj = Base.firstCell(AJEntityDailyClassActivity.GET_ASMT_OE_QUE_GRADE_STATUS, context.collectionId(),
                            sessionId, questions.get(AJEntityDailyClassActivity.RESOURCE_ID).toString());
                    if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t") || isGradedObj.toString().equalsIgnoreCase("true"))) {
                  	  qnData.put(JsonConstants.IS_GRADED, true);
                    } else {
                  	  qnData.put(JsonConstants.IS_GRADED, false);
                    }
                  } else {
                  	qnData.put(JsonConstants.IS_GRADED, true);
                  }
                qnData.put(JsonConstants.ANSWER_OBJECT, questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null
              		  ? new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()) : null);
                //Rubrics - Score should be NULL only incase of OE questions
                qnData.put(JsonConstants.SCORE, questions.get(AJEntityDailyClassActivity.SCORE) != null ?
              		  Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString())) : "NA");
                questionsArray.add(qnData);
              });
	          }
	          assessmentDataKPI.put(JsonConstants.QUESTIONS, questionsArray);
	          LOGGER.debug("Assessment question Attributes obtained");
	          contentArray.add(assessmentDataKPI);
	          LOGGER.debug("Done");
	        } else {
	          LOGGER.info("Assessment Attributes cannot be obtained");

	        }
	        resultBody.put(JsonConstants.CONTENT, contentArray);
	        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
	      } else {
	        LOGGER.info("SessionID Missing, Cannot Obtain Student Lesson Perf data");

	        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
	      }
	    }   // End ExecuteRequest()


	    @Override
	    public boolean handlerReadOnly() {
	        return true;
	    }

}
