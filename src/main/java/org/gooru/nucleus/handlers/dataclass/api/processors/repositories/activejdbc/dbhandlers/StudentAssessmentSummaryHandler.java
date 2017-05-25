package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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

/**
 * Created by mukul@gooru
 * 
 * modified by daniel
 */

public class StudentAssessmentSummaryHandler implements DBHandler {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentAssessmentSummaryHandler.class);

    private static final String REQUEST_SESSION_ID = "sessionId";
        
    private final ProcessorContext context;
  
    private String userId;
    
    private String sessionId;
    public StudentAssessmentSummaryHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Assessments");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Units"),
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
        Object classID = Base.firstCell(AJEntityBaseReports.SELECT_CLASS_BY_SESSION_ID, sessionId);
        LOGGER.debug("classID : {}", classID);
        if (classID == null) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("Independent Learner data can't be fetched by teacher/collaborator"),
                  ExecutionStatus.FAILED);
        } else {
          List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,classID, this.context.userIdFromSession());
          if (owner.isEmpty()) {
            LOGGER.debug("validateRequest() FAILED");
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

      this.userId = context.userIdFromSession();
      LOGGER.debug("UID is " + this.userId);
      this.sessionId = this.context.request().getString(REQUEST_SESSION_ID);
      JsonArray contentArray = new JsonArray();
      // STUDENT PERFORMANCE REPORTS IN ASSESSMENTS when SessionID NOT NULL
      if (!StringUtil.isNullOrEmpty(sessionId)) {
        List<Map> assessmentKPI = Base.findAll(AJEntityBaseReports.SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID, sessionId , AJEntityBaseReports.ATTR_CP_EVENTNAME);
  
        LOGGER.info("cID : {} , SID : {} ", context.collectionId(), sessionId);
        if (!assessmentKPI.isEmpty()) {
          LOGGER.debug("Assessment Attributes obtained");
          assessmentKPI.stream().forEach(m -> {
            JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentAttributesMap(), m);
            assessmentData.put(JsonConstants.SCORE, Math.round(Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString())));
            assessmentDataKPI.put(JsonConstants.ASSESSMENT, assessmentData);
          });
          
          LOGGER.debug("Assessment question Attributes started");

          List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID,
                  sessionId, AJEntityBaseReports.ATTR_CRP_EVENTNAME);
          
          JsonArray questionsArray = new JsonArray();
          if(!assessmentQuestionsKPI.isEmpty()){
            assessmentQuestionsKPI.stream().forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), questions);
              //FIXME :: This is to be revisited. We should alter the schema column type from TEXT to JSONB. After this change we can remove this logic
              qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
              qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())));
              questionsArray.add(qnData);
            });
          }
          //JsonArray questionsArray = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), assessmentQuestionsKPI);
          assessmentDataKPI.put(JsonConstants.QUESTIONS, questionsArray);
          LOGGER.debug("Assessment question Attributes obtained");
          contentArray.add(assessmentDataKPI);
          LOGGER.debug("Done");
        } else {
          LOGGER.info("Assessment Attributes cannot be obtained");
          // Return empty resultBody object instead of an error
          // return new
          // ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(),
          // ExecutionStatus.FAILED);
        }
        resultBody.put(JsonConstants.CONTENT, contentArray);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
      } else {
        LOGGER.info("SessionID Missing, Cannot Obtain Student Lesson Perf data");
        // Return empty resultBody object instead of an error
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
      }
    }   // End ExecuteRequest()
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
}
