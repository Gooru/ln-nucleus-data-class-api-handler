package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
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
 * Modified by daniel
 */

public class StudentCollectionSummaryHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentCollectionSummaryHandler.class);
    private final ProcessorContext context;
    private String userId;
    private double maxScore = 0 ;
    private long lastAccessedTime;
    private String sessionId;

    public StudentCollectionSummaryHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Collections");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Collections"),
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
        String classId = context.request().getString(EventConstants.CLASS_GOORU_OID);
        if (classId == null) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("Independent Learner data can't be fetched by teacher/collaborator"),
                  ExecutionStatus.FAILED);
        } else {
          List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, classId, this.context.userIdFromSession());
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

      this.userId = context.getUserIdFromRequest();
      LOGGER.debug("User ID is " + this.userId);
      String classId = context.request().getString(EventConstants.CLASS_GOORU_OID);
      String courseId = context.request().getString(EventConstants.COURSE_GOORU_OID);
      String unitId = context.request().getString(EventConstants.UNIT_GOORU_OID);
      String lessonId = context.request().getString(EventConstants.LESSON_GOORU_OID);
      String collectionId = context.collectionId();
      JsonArray contentArray = new JsonArray();

      LOGGER.debug("cID : {} , ClassID : {} ", collectionId, classId);
        //Getting Question Count
        List<Map> collectionMaximumScore;
        if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
          collectionMaximumScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_MAX_SCORE, classId,courseId,unitId,lessonId,collectionId,this.userId);
        }else{
          collectionMaximumScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_MAX_SCORE_, collectionId,this.userId);
        }

        collectionMaximumScore.forEach(ms -> {
        	if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
        		this.maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
        	} else {
        		this.maxScore = 0;
        	}
        });

        List<Map> lastAccessedTime;
        if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
          lastAccessedTime = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_LAST_ACCESSED_TIME, classId,courseId,unitId,lessonId,collectionId,this.userId);
        }else{
          lastAccessedTime = Base.findAll(AJEntityBaseReports.SELECT_CLASS_COLLECTION_LAST_ACCESSED_TIME, collectionId,this.userId);
        }

        if (!lastAccessedTime.isEmpty()) {
        	lastAccessedTime.forEach(l -> {
        		this.lastAccessedTime = l.get(AJEntityBaseReports.UPDATE_TIMESTAMP) != null ?
        				Timestamp.valueOf(l.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString()).getTime() : null;
        		this.sessionId = l.get(AJEntityBaseReports.SESSION_ID) != null ?
        				l.get(AJEntityBaseReports.SESSION_ID).toString() : "NA";
            });
        }

        List<Map> collectionData;
        if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
          collectionData = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_AGG_DATA, classId,courseId,unitId,lessonId,collectionId,this.userId);
        }else{
          collectionData = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_AGG_DATA_,collectionId,this.userId);
        }
        if (!collectionData.isEmpty()) {
          LOGGER.debug("Collection Attributes obtained");
          collectionData.forEach(m -> {
            JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionAttributesMap(), m);
            assessmentData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
            assessmentData.put(EventConstants.SESSION_ID, this.sessionId);
            assessmentData.put(EventConstants.RESOURCE_TYPE, AJEntityBaseReports.ATTR_COLLECTION);
            assessmentData.put(JsonConstants.SCORE, m.get(AJEntityBaseReports.SCORE) != null ? 
            		Math.round(Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString())) : null);

            //With Rubrics Score can be Null (for FR questions)
            double scoreInPercent;
            int reaction=0;
              Object collectionScore;
              if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
               collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE, classId,courseId,unitId,lessonId,collectionId,this.userId);
              }else{
               collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE_,collectionId,this.userId);
              }

              if(collectionScore != null && (this.maxScore > 0)){
                scoreInPercent =  ((Double.valueOf(collectionScore.toString()) / this.maxScore) * 100);
                assessmentData.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
              } else {
            	  assessmentData.putNull(AJEntityBaseReports.SCORE);
              }

            Object collectionReaction;
            if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
              collectionReaction = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_REACTION, classId,courseId,unitId,lessonId,collectionId,this.userId);
            }else{
              collectionReaction = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_REACTION_,collectionId,this.userId);
            }
            if(collectionReaction != null){
              reaction = Integer.valueOf(collectionReaction.toString());
            }
            LOGGER.debug("Collection reaction : {} - collectionId : {}" , reaction, collectionId);
            assessmentData.put(AJEntityBaseReports.ATTR_REACTION, (reaction));
            assessmentDataKPI.put(JsonConstants.COLLECTION, assessmentData);
          });
          LOGGER.debug("Collection resource Attributes started");
          List<Map> assessmentQuestionsKPI;
          if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
            assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA,
                  classId,courseId,unitId,lessonId,collectionId,this.userId);
          }else{
             assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA_
                    ,collectionId,this.userId);
          }
          JsonArray questionsArray = new JsonArray();
          if(!assessmentQuestionsKPI.isEmpty()){
            assessmentQuestionsKPI.forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
              //FIXME :: This is to be revisited. We should alter the schema column type from TEXT to JSONB. After this change we can remove this logic
              if(questions.get(AJEntityBaseReports.ANSWER_OBECT) != null){
                qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
              }
              //Default answerStatus will be skipped
              if(qnData.getString(EventConstants.RESOURCE_TYPE).equalsIgnoreCase(EventConstants.QUESTION)){
                qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
              }
              //qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())));
                List<Map> questionScore;
                if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
                  questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE, classId,courseId,unitId,lessonId,collectionId,questions.get(AJEntityBaseReports.RESOURCE_ID),this.userId);
                }else{
                  questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE_, collectionId,
                		  questions.get(AJEntityBaseReports.RESOURCE_ID),this.userId);
                }
                String latestSessionId = null;
                if(!questionScore.isEmpty()){
                    latestSessionId = (questionScore.get(0).get(AJEntityBaseReports.SESSION_ID) != null) ? questionScore.get(0).get(AJEntityBaseReports.SESSION_ID).toString() : null;
                questionScore.forEach(qs -> {
                    qnData.put(JsonConstants.ANSWER_OBJECT, qs.get(AJEntityBaseReports.ANSWER_OBECT) != null
                  		  ? new JsonArray(qs.get(AJEntityBaseReports.ANSWER_OBECT).toString()) : null);
                    //Rubrics - Score may be NULL only incase of OE questions
                    qnData.put(JsonConstants.SCORE, qs.get(AJEntityBaseReports.SCORE) != null ?
                    		Math.round(Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString()) * 100) : "NA");
                  qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityBaseReports.ATTR_ATTEMPT_STATUS).toString());
                });
                }
              //Get grading status for Questions
              if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) &&
            		  !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
            	  if (latestSessionId != null && qnData.getString(EventConstants.QUESTION_TYPE).equalsIgnoreCase(EventConstants.OPEN_ENDED_QUE)){
                      Object isGradedObj = Base.firstCell(AJEntityBaseReports.GET_OE_QUE_GRADE_STATUS, collectionId, latestSessionId, questions.get(AJEntityBaseReports.RESOURCE_ID));
                      if (isGradedObj != null && (isGradedObj.toString().equalsIgnoreCase("t") || isGradedObj.toString().equalsIgnoreCase("true"))) {
                    	  qnData.put(JsonConstants.IS_GRADED, true);
                      } else {
                    	  qnData.put(JsonConstants.IS_GRADED, false);
                      }
                    } else {
                    	qnData.put(JsonConstants.IS_GRADED, true);
                    }
              }

              List<Map> resourceReaction;
              if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
                resourceReaction = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_REACTION, classId,courseId,unitId,lessonId,collectionId,questions.get(AJEntityBaseReports.RESOURCE_ID),this.userId);
              }else{
                resourceReaction = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_REACTION_, collectionId,questions.get(AJEntityBaseReports.RESOURCE_ID),this.userId);
              }
              if(!resourceReaction.isEmpty()){
              resourceReaction.forEach(rs ->{
                qnData.put(JsonConstants.REACTION, Integer.valueOf(rs.get(AJEntityBaseReports.REACTION).toString()));
                LOGGER.debug("Resource reaction: {} - resourceId : {}" ,rs.get(AJEntityBaseReports.REACTION).toString(), questions.get(AJEntityBaseReports.RESOURCE_ID));
              });
              }
              qnData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
              qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
              questionsArray.add(qnData);
            });
          }
          //JsonArray questionsArray = ValueMapper.map(ResponseAttributeIdentifier.getSessionAssessmentQuestionAttributesMap(), assessmentQuestionsKPI);
          assessmentDataKPI.put(JsonConstants.RESOURCES, questionsArray);
          LOGGER.debug("Collection Attributes obtained");
          contentArray.add(assessmentDataKPI);
        } else {
          LOGGER.info("Collection Attributes cannot be obtained");
        }
        resultBody.put(JsonConstants.CONTENT, contentArray);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

    }

    @Override
    public boolean handlerReadOnly() {
        return true;
    }
}
