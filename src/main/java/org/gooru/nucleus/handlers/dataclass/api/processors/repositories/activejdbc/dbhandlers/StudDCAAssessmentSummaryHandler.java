package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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
	  
	    private String userId;
	    
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
	    // No Teacher validation yet needed	
	      if (context.getUserIdFromRequest() == null
	              || (!context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
	          LOGGER.debug("validateRequest() FAILED");
	          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User Auth failed"),
	                  ExecutionStatus.FAILED);
	      }
	      LOGGER.debug("validateRequest() OK");
	      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	    }

	    @Override
	    @SuppressWarnings("rawtypes")
	    public ExecutionResult<MessageResponse> executeRequest() {
	      JsonObject resultBody = new JsonObject();
	      JsonObject assessmentDataKPI = new JsonObject();

	      this.userId = context.getUserIdFromRequest();
	      
	      LOGGER.debug("UID is " + this.userId);
	      this.sessionId = this.context.request().getString(REQUEST_SESSION_ID);
	      JsonArray contentArray = new JsonArray();
	      
	      if (!StringUtil.isNullOrEmpty(sessionId)) {
	        List<Map> assessmentKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID, context.collectionId(), sessionId , AJEntityDailyClassActivity.ATTR_CP_EVENTNAME);
	        Object assessmentReactionObject =  Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_REACTION_AND_SESSION_ID, context.collectionId(), sessionId);

	        LOGGER.info("cID : {} , SID : {} ", context.collectionId(), sessionId);
	        if (!assessmentKPI.isEmpty()) {
	          LOGGER.debug("Assessment Attributes obtained");
	          assessmentKPI.stream().forEach(m -> {
	            JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCAAssessmentAttributesMap(), m);
	            assessmentData.put(JsonConstants.SCORE, Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString())));
	            assessmentData.put(JsonConstants.REACTION, ((Number)assessmentReactionObject).intValue());
	            assessmentDataKPI.put(JsonConstants.ASSESSMENT, assessmentData);
	          });
	          
	          LOGGER.debug("Assessment question Attributes started");

	          List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,context.collectionId(),
	                  sessionId, AJEntityDailyClassActivity.ATTR_CRP_EVENTNAME);
	          
	          JsonArray questionsArray = new JsonArray();
	          if(!assessmentQuestionsKPI.isEmpty()){
              assessmentQuestionsKPI.stream().forEach(questions -> {
                JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCAAssessmentQuestionAttributesMap(), questions);
                Object reactionObj = Base.firstCell(AJEntityDailyClassActivity.SELECT_ASSESSMENT_RESOURCE_REACTION, context.collectionId(), sessionId,
                        questions.get(AJEntityBaseReports.RESOURCE_ID).toString());
                qnData.put(JsonConstants.REACTION, reactionObj != null ? ((Number) reactionObj).intValue() : 0);
                qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()));
                qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString())));
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
