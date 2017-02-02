package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

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

public class StudentCollectionPerfHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentCollectionPerfHandler.class);
    private final ProcessorContext context;
    private String userId;

    public StudentCollectionPerfHandler(ProcessorContext context) {
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
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> creator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userIdFromSession());
        if (creator.isEmpty()) {
          List<Map> collaborator = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_COLLABORATOR, this.context.classId(), this.context.userIdFromSession());
          if (collaborator.isEmpty()) {
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
      String classId = context.request().getString(EventConstants.CLASS_GOORU_OID);
      String courseId = context.request().getString(EventConstants.COURSE_GOORU_OID);
      String unitId = context.request().getString(EventConstants.UNIT_GOORU_OID);
      String lessonId = context.request().getString(EventConstants.LESSON_GOORU_OID);
      String collectionId = context.collectionId();
      JsonArray contentArray = new JsonArray();
      
      LOGGER.info("cID : {} , ClassID : {} ", collectionId, classId);

      if (!StringUtil.isNullOrEmpty(classId) && !StringUtil.isNullOrEmpty(courseId) && !StringUtil.isNullOrEmpty(unitId) && !StringUtil.isNullOrEmpty(lessonId)) {
        List<Map> assessmentKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_FOREACH_COLLID_AND_SESSIONID, classId,courseId,unitId,lessonId,collectionId,this.userId,
                AJEntityBaseReports.ATTR_CP_EVENTNAME);
  
        if (!assessmentKPI.isEmpty()) {
          LOGGER.debug("COllection Attributes obtained");
          assessmentKPI.stream().forEach(m -> {
            JsonObject assessmentData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionAttributesMap(), m);
            assessmentDataKPI.put(JsonConstants.COLLECTION, assessmentData);
          });
          LOGGER.debug("Collection resource Attributes started");
          List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_RESOURCE_FOREACH_COLLID_AND_SESSIONID,
                  classId,courseId,unitId,lessonId,collectionId,this.userId, AJEntityBaseReports.ATTR_CRP_EVENTNAME);
          
          JsonArray questionsArray = new JsonArray();
          if(!assessmentQuestionsKPI.isEmpty()){
            assessmentQuestionsKPI.stream().forEach(questions -> {
              JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
              //FIXME :: This is to be revisited. We should alter the schema column type from TEXT to JSONB. After this change we can remove this logic
              if(questions.get(AJEntityBaseReports.ANSWER_OBECT) != null){
                qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
              }
              //FIXME :: it can be removed once we fix writer code.
              if(qnData.getString(JsonConstants.RESOURCE_TYPE) != null){
                qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.QUESTION);
              }else{
                qnData.put(JsonConstants.RESOURCE_TYPE, JsonConstants.RESOURCE);
              }
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
      } else {
        LOGGER.info("CUL IDs are Missing, Cannot Obtain Student Collection Perf data");
        // Return empty resultBody object instead of an error
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
      }
    }   

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
}
