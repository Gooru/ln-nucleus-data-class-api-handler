package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.sql.Timestamp;
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
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudDCACollectionSummaryHandler implements DBHandler {
	

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudDCACollectionSummaryHandler.class);
  private final ProcessorContext context;
  private static final String DATE = "date";
  private String userId;
  private int questionCount = 0 ;
  private long lastAccessedTime;
  
  public StudDCACollectionSummaryHandler(ProcessorContext context) {
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
                LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User Auth Failed"), ExecutionStatus.FAILED);      
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
    String classId = context.request().getString(MessageConstants.CLASS_ID);
    // For DCA activities, the summary report should be fetched based only on
    // classId and collectionId. (CourseId, UnitId and lessonId are not expected)
    String courseId = context.request().getString(MessageConstants.COURSE_ID);
    String unitId = context.request().getString(MessageConstants.UNIT_ID);
    String lessonId = context.request().getString(MessageConstants.LESSON_ID);
    String todayDate = this.context.request().getString(DATE);
    String collectionId = context.collectionId();
    JsonArray contentArray = new JsonArray();

    // For DCA activities, the summary report should be fetched based only on classId and collectionId. (CourseId, UnitId and lessonId are not expected)
//    if (StringUtil.isNullOrEmpty(classId) || StringUtil.isNullOrEmpty(courseId) || StringUtil.isNullOrEmpty(unitId) || StringUtil.isNullOrEmpty(lessonId)) {
    if (StringUtil.isNullOrEmpty(classId)) {
      LOGGER.warn("ClassId is mandatory to fetch Student Performance in a DCA Collection");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("ClassId Missing. Cannot fetch Collection Summary in DCA"),
              ExecutionStatus.FAILED);
    	
    }
    
    if (StringUtil.isNullOrEmpty(todayDate)) {
        LOGGER.warn("Date is mandatory to fetch Student Performance in a DCA Collection");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Date Missing. Cannot fetch Collection Summary in DCA"),
                ExecutionStatus.FAILED);        
      	
      }
    
    LOGGER.debug("cID : {} , ClassID : {} ", collectionId, classId);
    
    Date date = Date.valueOf(todayDate);    
      //Getting Question Count 
      List<Map> collectionQuestionCount = null;
        collectionQuestionCount = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_QUESTION_COUNT, classId, collectionId, this.userId, date);

      //If questions are not present then Question Count is always zero, however this additional check needs to be added
      //since during migration of data from 3.0 chances are that QC may be null instead of zero
      collectionQuestionCount.forEach(qc -> {
      	if (qc.get(AJEntityDailyClassActivity.QUESTION_COUNT) != null) {
      		this.questionCount = Integer.valueOf(qc.get(AJEntityDailyClassActivity.QUESTION_COUNT).toString());
      	} else {
      		this.questionCount = 0;
      	}
        this.lastAccessedTime = Timestamp.valueOf(qc.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString()).getTime();
      });
      List<Map> collectionData = null;      
        collectionData = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_DATA, classId, collectionId, this.userId, date);
      
      if (!collectionData.isEmpty()) {
        LOGGER.debug("Collection Attributes obtained");
        collectionData.stream().forEach(m -> {
          JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCACollectionAttributesMap(), m);
          assessmentData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
          assessmentData.put(EventConstants.SESSION_ID, EventConstants.NA);
          //Update this to be COLLECTION_TYPE in response
          assessmentData.put(EventConstants.COLLECTION_TYPE, AJEntityDailyClassActivity.ATTR_COLLECTION);
          assessmentData.put(JsonConstants.SCORE, Math.round(Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString())));

          double scoreInPercent=0;
          int reaction=0;
          if(this.questionCount > 0){
            Object collectionScore = null;            
             collectionScore = Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_SCORE, classId, collectionId, this.userId, date);
            
            if(collectionScore != null){
              scoreInPercent =  (((double) Double.valueOf(collectionScore.toString()) / this.questionCount) * 100);
            }
          }
          LOGGER.debug("Collection score : {} - collectionId : {}" , Math.round(scoreInPercent), collectionId);
          assessmentData.put(AJEntityDailyClassActivity.SCORE, Math.round(scoreInPercent)); 
          Object collectionReaction = null;          
            collectionReaction = Base.firstCell(AJEntityDailyClassActivity.SELECT_COLLECTION_AGG_REACTION, classId, collectionId, this.userId, date);
          
          if(collectionReaction != null){
            reaction = Integer.valueOf(collectionReaction.toString());
          }
          LOGGER.debug("Collection reaction : {} - collectionId : {}" , reaction, collectionId);
          assessmentData.put(AJEntityDailyClassActivity.ATTR_REACTION, (reaction));
          assessmentDataKPI.put(JsonConstants.COLLECTION, assessmentData);
        });
        LOGGER.debug("Collection resource Attributes started");
        List<Map> assessmentQuestionsKPI = null;        
          assessmentQuestionsKPI = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_DATA,
                classId, collectionId, this.userId, date);
        
        JsonArray questionsArray = new JsonArray();
        if(!assessmentQuestionsKPI.isEmpty()){
          assessmentQuestionsKPI.stream().forEach(questions -> {
            JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionDCACollectionResourceAttributesMap(), questions);
            if(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT) != null){
              qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()));
            }
            //Default answerStatus will be skipped
            if(qnData.getString(EventConstants.RESOURCE_TYPE).equalsIgnoreCase(EventConstants.QUESTION)){
              qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
            }
            qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityDailyClassActivity.SCORE).toString())));
            if(this.questionCount > 0){
              List<Map> questionScore = null;              
                questionScore = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_QUESTION_AGG_SCORE, classId, collectionId, 
                		questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId, date);
              
              if(!questionScore.isEmpty()){
              questionScore.forEach(qs ->{
                qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(qs.get(AJEntityDailyClassActivity.SCORE).toString()) * 100));
                qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(qs.get(AJEntityDailyClassActivity.ANSWER_OBJECT).toString()));
                qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityDailyClassActivity.ATTR_ATTEMPT_STATUS).toString());                
              });
              }
             }
            List<Map> resourceReaction = null;            
              resourceReaction = Base.findAll(AJEntityDailyClassActivity.SELECT_COLLECTION_RESOURCE_AGG_REACTION, classId, collectionId, 
            		  questions.get(AJEntityDailyClassActivity.RESOURCE_ID), this.userId, date);
            
            if(!resourceReaction.isEmpty()){
            resourceReaction.forEach(rs ->{
              qnData.put(JsonConstants.REACTION, Integer.valueOf(rs.get(AJEntityDailyClassActivity.REACTION).toString()));              
            });
            }
            qnData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
            qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
            questionsArray.add(qnData);
          });
        }
        assessmentDataKPI.put(JsonConstants.RESOURCES, questionsArray);
        LOGGER.debug("Collection Attributes obtained");
        contentArray.add(assessmentDataKPI);
        LOGGER.debug("Done");
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
