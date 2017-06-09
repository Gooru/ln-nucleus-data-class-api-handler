package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 * Modified by daniel
 */

  public class StudentPeersInLessonHandler implements DBHandler {
  
    private static final Logger LOGGER = LoggerFactory.getLogger(StudentPeersInUnitHandler.class);
  
    private final ProcessorContext context;
  
    private static final long timeDiff = 900000;
  
    String cId = new String();
    String user = new String();
    Integer activeUser = 0;
    Integer inactiveUser = 0;
  
    public StudentPeersInLessonHandler(ProcessorContext context) {
      this.context = context;
    }
  
    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
  
      // No Sanity Check required since, no params are being passed in Request
      // Body
  
      LOGGER.debug("checkSanity() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }
  
    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }
  
    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
      JsonObject resultBody = new JsonObject(); 
      JsonArray peerArray = new JsonArray();
  
      // If CollectionType is Assessment
      //@NU Resource as Suggestions - include event_name = 'collection.play' to ensure peers at C/A level
      LazyList<AJEntityBaseReports> collIDforAssessment =
              AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_PEERS_IN_LESSON, context.classId(),this.context.userIdFromSession(),
                      context.courseId(), context.unitId(), context.lessonId());
  
      if (!collIDforAssessment.isEmpty()) {
        LOGGER.debug("Got a list of Distinct collectionIDs for this lesson");
  
        collIDforAssessment.forEach(coll -> {

          String assessmentId = coll.get(AJEntityBaseReports.COLLECTION_OID).toString();
          String userId = coll.get(AJEntityBaseReports.GOORUUID).toString();
          String timestamp = coll.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString();
          String collectionType = coll.get(AJEntityBaseReports.COLLECTION_TYPE).toString();
          String collTypeParam = collectionType.equalsIgnoreCase(JsonConstants.COLLECTION) ? JsonConstants.COLLECTIONID : JsonConstants.ASSESSMENTID;
          LOGGER.debug("the User is " + user);
          LOGGER.debug("The Value of CollectionID, should be same as assessmentId " + cId);
            Timestamp currTs = new Timestamp(System.currentTimeMillis());              
            Timestamp userTs = Timestamp.valueOf(timestamp);
            if (((currTs.getTime() - userTs.getTime()) < timeDiff)) {
              peerArray.add(new JsonObject().put(collTypeParam, assessmentId).put(JsonConstants.USERUID, userId).put(JsonConstants.STATUS,
                      JsonConstants.ACTIVE));
            } else if (((currTs.getTime() - userTs.getTime()) > timeDiff)) {
              peerArray.add(new JsonObject().put(collTypeParam, assessmentId).put(JsonConstants.USERUID, userId).put(JsonConstants.STATUS,
                      JsonConstants.INACTIVE));
            }
        
         });
  
      }
  
      resultBody.put(JsonConstants.CONTENT, peerArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);  
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
  
    }
  
    @Override
    public boolean handlerReadOnly() {
      return false;
    }

}
