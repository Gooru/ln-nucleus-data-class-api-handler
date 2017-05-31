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
 * 
 * Modified by daniel
 */

public class StudentCollectionSummaryHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentCollectionSummaryHandler.class);
    private final ProcessorContext context;
    private String userId;
    private int questionCount = 0 ;
    private long lastAccessedTime;
    
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
        List<Map> collectionQuestionCount = null;
        if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
          collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_COUNT, classId,courseId,unitId,lessonId,collectionId,this.userId);
        }else{
          collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_COUNT_, collectionId,this.userId);
        }
        collectionQuestionCount.forEach(qc -> {
          this.questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
          this.lastAccessedTime = Timestamp.valueOf(qc.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString()).getTime();
        });
        List<Map> collectionData = null;
        if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
          collectionData = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_AGG_DATA, classId,courseId,unitId,lessonId,collectionId,this.userId);
        }else{
          collectionData = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_AGG_DATA_,collectionId,this.userId);          
        }
        if (!collectionData.isEmpty()) {
          LOGGER.debug("Collection Attributes obtained");
          collectionData.stream().forEach(m -> {
            JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionAttributesMap(), m);
            assessmentData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
            assessmentData.put(EventConstants.SESSION_ID, EventConstants.NA);
            assessmentData.put(EventConstants.RESOURCE_TYPE, AJEntityBaseReports.ATTR_COLLECTION);
            assessmentData.put(JsonConstants.SCORE, Math.round(Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString())));

            double scoreInPercent=0;
            int reaction=0;
            if(this.questionCount > 0){
              Object collectionScore = null;
              if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
               collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE, classId,courseId,unitId,lessonId,collectionId,this.userId);
              }else{
               collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE_,collectionId,this.userId);
              }
              if(collectionScore != null){
                scoreInPercent =  (((double) Double.valueOf(collectionScore.toString()) / this.questionCount) * 100);
              }
            }
            LOGGER.debug("Collection score : {} - collectionId : {}" , Math.round(scoreInPercent), collectionId);
            assessmentData.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent)); 
            Object collectionReaction = null;
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
          List<Map> assessmentQuestionsKPI = null;
          if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
            assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA,
                  classId,courseId,unitId,lessonId,collectionId,this.userId);
          }else{
             assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA_
                    ,collectionId,this.userId);
          }
          JsonArray questionsArray = new JsonArray();
          if(!assessmentQuestionsKPI.isEmpty()){
            assessmentQuestionsKPI.stream().forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
              //FIXME :: This is to be revisited. We should alter the schema column type from TEXT to JSONB. After this change we can remove this logic
              if(questions.get(AJEntityBaseReports.ANSWER_OBECT) != null){
                qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
              }
              //Default answerStatus will be skipped
              if(qnData.getString(EventConstants.RESOURCE_TYPE).equalsIgnoreCase(EventConstants.QUESTION)){
                qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
              }
              qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())));
              if(this.questionCount > 0){
                List<Map> questionScore = null;
                if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
                  questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE, classId,courseId,unitId,lessonId,collectionId,questions.get(AJEntityBaseReports.RESOURCE_ID),this.userId);
                }else{
                  questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE_, collectionId,questions.get(AJEntityBaseReports.RESOURCE_ID),this.userId);
                }
                if(!questionScore.isEmpty()){
                questionScore.forEach(qs ->{
                  qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString()) * 100));
                  qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(qs.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
                  qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityBaseReports.ATTR_ATTEMPT_STATUS).toString());
                  LOGGER.debug("Question Score : {} - resourceId : {}" ,qs.get(AJEntityBaseReports.SCORE).toString(), questions.get(AJEntityBaseReports.RESOURCE_ID));
                });
                }
               }
              List<Map> resourceReaction = null;
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
          LOGGER.debug("Done");
        } else {
          LOGGER.info("Collection Attributes cannot be obtained");
          // Return empty resultBody object instead of an error
          // return new
          // ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(),
          // ExecutionStatus.FAILED);
        }
        resultBody.put(JsonConstants.CONTENT, contentArray);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
      
    }   

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
}
