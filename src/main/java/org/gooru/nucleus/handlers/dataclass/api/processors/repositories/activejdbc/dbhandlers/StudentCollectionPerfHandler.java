package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
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

public class StudentCollectionPerfHandler implements DBHandler {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentCollectionPerfHandler.class);
	    private static final String REQUEST_USERID = "userUid";
	    private final ProcessorContext context;

	    public StudentCollectionPerfHandler(ProcessorContext context) {
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
	    	  LOGGER.info("the class_id is " + context.classId());
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
	        
	    String collectionType = "collection";
	    String classId = this.context.classId();
	    LOGGER.info("the class_id is" + classId);
	    LOGGER.info("the class_id is" + this.context.classId());
	    String courseId = this.context.courseId();
	    LOGGER.info("the course_id is" + courseId);
	    LOGGER.info("the course_id is" + this.context.courseId());
	    String unitId = this.context.unitId();
	    String lessonId = this.context.lessonId();
	    String collectionId = this.context.collectionId();
	    JsonArray contentArray = new JsonArray();

	    String userId = this.context.request().getString(REQUEST_USERID);

	    List<String> userIds = new ArrayList<>();

	    if (this.context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
	      LOGGER.warn("UserID is not in the request to fetch Student Performance in Lesson. Assume user is a teacher");
	      LazyList<AJEntityBaseReports> userIdforlesson =
	              AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_COLLECTION_ID_FILTERBY_COLLTYPE, context.classId(),
	                      context.courseId(), context.unitId(), context.lessonId(), context.collectionId(), collectionType);
	      userIdforlesson.forEach(coll -> userIds.add(coll.getString(AJEntityBaseReports.GOORUUID)));

	    } else {
	      userIds.add(userId);
	    }

	    LOGGER.debug("UID is " + userId);

	    for (String userID : userIds) {
          LOGGER.debug("Collection resource Attributes started");
          List<Map> collectionQuestionsKPI;
          if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
            collectionQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA,
                  classId,courseId,unitId,lessonId,collectionId, userID);
          }else{
             collectionQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_DATA_
                    ,collectionId, userID);
          }
          if(!collectionQuestionsKPI.isEmpty()){
              JsonObject contentBody = new JsonObject();
        	JsonArray questionsArray = new JsonArray();
            collectionQuestionsKPI.forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
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
                  questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE, classId,courseId,unitId,lessonId,collectionId,
                		  questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
                }else{
                  questionScore = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_AGG_SCORE_, collectionId,
                		  questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
                }
                String latestSessionId = null;
                if(!questionScore.isEmpty()){
                    latestSessionId = (questionScore.get(0).get(AJEntityBaseReports.SESSION_ID) != null) ? questionScore.get(0).get(AJEntityBaseReports.SESSION_ID).toString() : null;
                questionScore.forEach(qs -> {
                    qnData.put(JsonConstants.ANSWER_OBJECT, qs.get(AJEntityBaseReports.ANSWER_OBECT) != null
                  		  ? new JsonArray(qs.get(AJEntityBaseReports.ANSWER_OBECT).toString()) : null);
                    //Rubrics - Score may be NULL only incase of OE questions
                    qnData.put(JsonConstants.SCORE, qs.get(AJEntityBaseReports.SCORE) != null ?
                    		Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString()) : "NA");
                  qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityBaseReports.ATTR_ATTEMPT_STATUS).toString());
                  qnData.put(JsonConstants.MAX_SCORE, qs.get(AJEntityBaseReports.MAX_SCORE) != null ?
                  		Double.valueOf(qs.get(AJEntityBaseReports.MAX_SCORE).toString()) : "NA");
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
                    	qnData.put(JsonConstants.IS_GRADED, "NA");
                    }
            	  
              }

              List<Map> resourceReaction;
              if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
                resourceReaction = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_REACTION, classId,courseId,unitId,lessonId,collectionId,
                		questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
              }else{
                resourceReaction = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_AGG_REACTION_, collectionId,
                		questions.get(AJEntityBaseReports.RESOURCE_ID), userID);
              }
              if(!resourceReaction.isEmpty()){
              resourceReaction.forEach(rs ->{
                qnData.put(JsonConstants.REACTION, Integer.valueOf(rs.get(AJEntityBaseReports.REACTION).toString()));
                LOGGER.debug("Resource reaction: {} - resourceId : {}" ,rs.get(AJEntityBaseReports.REACTION).toString(), questions.get(AJEntityBaseReports.RESOURCE_ID));
              });
              }
              //qnData.put(EventConstants.EVENT_TIME, this.lastAccessedTime);
              qnData.put(EventConstants.SESSION_ID, EventConstants.NA);
              questionsArray.add(qnData);             
              
            });
            
            contentBody.put(JsonConstants.USAGE_DATA, questionsArray).put(JsonConstants.USERUID, userID);
            resultarray.add(contentBody);          
          } 
	    }
	    resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

	  }

	    @Override
	    public boolean handlerReadOnly() {
	        return true;
	    }

}
