    package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;
    
    import java.util.ArrayList;
import java.util.Iterator;
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
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
    
    /**
     * Created by mukul@gooru Modified by daniel
     */
    
    public class StudentUnitPerfHandler implements DBHandler {
    
      private static final Logger LOGGER = LoggerFactory.getLogger(StudentUnitPerfHandler.class);
      private static final String REQUEST_COLLECTION_TYPE = "collectionType";
      private static final String REQUEST_USERID = "userUid";
    
      private final ProcessorContext context;
    
      private String collectionType;
      private String userId;
    
      // For stuffing Json
      private String lessonId;
      private long questionCount;
      
      public StudentUnitPerfHandler(ProcessorContext context) {
        this.context = context;
      }
    
      @Override
      public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
          LOGGER.warn("invalid request received to fetch Student Performance in Units");
          return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in Units"),
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
    
        // TODO Confirm if collType is optional. In which case we need not check for
        // null or Empty (and probably send data for both)
        this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
        if (StringUtil.isNullOrEmpty(collectionType)) {
          LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Course");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in course"),
                  ExecutionStatus.FAILED);
        }
        LOGGER.debug("Collection Type is " + this.collectionType);
    
        this.userId = this.context.request().getString(REQUEST_USERID);
    
        List<String> userIds = new ArrayList<>();
        List<String> lessonIds = new ArrayList<>();
        if (StringUtil.isNullOrEmpty(userId)) {
          LOGGER.warn("UserID is not in the request to fetch Student Performance in Course. Asseume user is a teacher");
          LazyList<AJEntityBaseReports> userIdforUnit =
                  AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_UNIT_ID_FITLERBY_COLLTYPE, context.classId(),
                          context.courseId(), context.unitId(), this.collectionType);
          userIdforUnit.forEach(lesson -> userIds.add(lesson.getString(AJEntityBaseReports.GOORUUID)));
    
        } else {
          userIds.add(this.userId);
        }
        for (String userID : userIds) {
          JsonObject contentBody = new JsonObject();
          JsonArray UnitKpiArray = new JsonArray();
    
          LazyList<AJEntityBaseReports> lessonIDforUnit = null;
            lessonIDforUnit = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_LESSON_ID_FOR_UNIT_ID_FITLERBY_COLLTYPE, context.classId(),
                          context.courseId(), context.unitId(), this.collectionType, userID);
          if (!lessonIDforUnit.isEmpty()) {
            LOGGER.debug("Got a list of Distinct lessonIDs for this Unit");
    
            lessonIDforUnit.forEach(lesson -> lessonIds.add(lesson.getString(AJEntityBaseReports.LESSON_GOORU_OID)));
            List<Map> lessonKpi = null;
            if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
              lessonKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_UNIT_PERF_FOR_COLLECTION, context.classId(), context.courseId(),
                    context.unitId(), this.collectionType, userID, listToPostgresArrayString(lessonIds));
            }else{
              lessonKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_UNIT_PERF_FOR_ASSESSMENT, context.classId(), context.courseId(),
                      context.unitId(), this.collectionType, userID, listToPostgresArrayString(lessonIds), EventConstants.COLLECTION_PLAY);
            }
            if (!lessonKpi.isEmpty()) {
              lessonKpi.forEach(m -> {
                this.lessonId = m.get(AJEntityBaseReports.ATTR_LESSON_ID).toString();
                LOGGER.debug("The Value of LESSONID " + lessonId);
                List<Map> completedCountMap = null;
                if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
                  completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLL_COUNT_FOREACH_LESSON_ID, context.classId(),
                        context.courseId(), context.unitId(), this.lessonId, this.collectionType, userID, EventConstants.COLLECTION_PLAY);
                }else{
                  completedCountMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLLID_COUNT_FOREACH_LESSON_ID, context.classId(),
                          context.courseId(), context.unitId(), this.lessonId, this.collectionType, userID, EventConstants.COLLECTION_PLAY);   
                }
                JsonObject lessonData = ValueMapper.map(ResponseAttributeIdentifier.getUnitPerformanceAttributesMap(), m);
                completedCountMap.forEach( scoreCompletonMap -> {
                  lessonData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
                  lessonData.put(AJEntityBaseReports.ATTR_SCORE,  Math.round(Double.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_SCORE).toString())));
                  LOGGER.debug("UnitID : {} - UserID : {} - Score : {}",lessonId,userID, Math.round(Double.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_SCORE).toString())));
                  LOGGER.debug("UnitID : {} - UserID : {} - completedCount : {}",lessonId,userID,Integer.valueOf(scoreCompletonMap.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));

                });
                // FIXME: Total count will be taken from nucleus core.
                lessonData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
                // FIXME : Revisit this logic in future.
                if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
                  lessonData.put(EventConstants.VIEWS, lessonData.getInteger(EventConstants.ATTEMPTS));
                  lessonData.remove(EventConstants.ATTEMPTS);
                }
                JsonArray assessmentArray = new JsonArray();
                LazyList<AJEntityBaseReports> collIDforlesson = null;
                  collIDforlesson =  AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_COLLID_FOR_LESSON_ID_FILTERBY_COLLTYPE, context.classId(),
                                context.courseId(), context.unitId(), this.lessonId, this.collectionType, userID);
    
                List<String> collIds = new ArrayList<>();
                if (!collIDforlesson.isEmpty()) {
                  LOGGER.info("Got a list of Distinct collectionIDs for this lesson");
                  collIDforlesson.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                }
                List<Map> assessmentKpi = null;
                if(this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)){
                  assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION, context.classId(),
                          context.courseId(), context.unitId(), this.lessonId, listToPostgresArrayString(collIds), userID);
                }else{
                  assessmentKpi = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT, context.classId(),
                        context.courseId(), context.unitId(), this.lessonId, listToPostgresArrayString(collIds), userID, EventConstants.COLLECTION_PLAY);
                }
                if (!assessmentKpi.isEmpty()) {
                  assessmentKpi.forEach(ass -> {
                    JsonObject assData = ValueMapper.map(ResponseAttributeIdentifier.getLessonPerformanceAttributesMap(), ass);
                    // FIXME : revisit completed count and total count
                    assData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 1);
                    assData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
                    assData.put(AJEntityBaseReports.ATTR_SCORE, Math.round(Double.valueOf(ass.get(AJEntityBaseReports.ATTR_SCORE).toString())));
                    // FIXME: This logic to be revisited.
                    if (this.collectionType.equalsIgnoreCase(JsonConstants.COLLECTION)) {
                      List<Map> collectionQuestionCount = null;
                        collectionQuestionCount = Base.findAll(AJEntityBaseReports.SELECT_COLLECTION_QUESTION_COUNT, context.classId(),
                              context.courseId(), context.unitId(), this.lessonId, assData.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID),this.userId);
                      collectionQuestionCount.forEach(qc -> {
                        this.questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
                      });
                      double scoreInPercent=0;
                      if(this.questionCount > 0){
                        Object collectionScore = null;
                          collectionScore = Base.firstCell(AJEntityBaseReports.SELECT_COLLECTION_AGG_SCORE, context.classId(),
                                context.courseId(), context.unitId(), this.lessonId, assData.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID),this.userId);
                        if(collectionScore != null){
                          scoreInPercent =  (((double) Double.valueOf(collectionScore.toString()) / this.questionCount) * 100);
                        }
                      } 
                      assData.put(AJEntityBaseReports.ATTR_SCORE, Math.round(scoreInPercent));
                      assData.put(AJEntityBaseReports.ATTR_COLLECTION_ID, assData.getString(AJEntityBaseReports.ATTR_ASSESSMENT_ID));
                      assData.remove(AJEntityBaseReports.ATTR_ASSESSMENT_ID);
                      assData.put(EventConstants.VIEWS, assData.getInteger(EventConstants.ATTEMPTS));
                      assData.remove(EventConstants.ATTEMPTS);
                    }
                    assessmentArray.add(assData);
                  });
                }
                lessonData.put(JsonConstants.SOURCELIST, assessmentArray);
                UnitKpiArray.add(lessonData);
    
              });
            } else {
              LOGGER.info("No data returned for Student Perf in Assessment");
              // return new
              // ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(),
              // ExecutionStatus.FAILED);
            }
    
          } else {
            LOGGER.info("Could not get Student Unit Performance");
            // Return an empty resultBody instead of an Error
            // return new
            // ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(),
            // ExecutionStatus.FAILED);
          }
    
          contentBody.put(JsonConstants.USAGE_DATA, UnitKpiArray).put(JsonConstants.USERUID, userID);
          resultarray.add(contentBody);
        }
        resultBody.put(JsonConstants.CONTENT, resultarray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
    
      }
    
      @Override
      public boolean handlerReadOnly() {
        return false;
      }
    
      private String listToPostgresArrayString(List<String> input) {
        int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
                                                    // 36
                                                    // chars
        Iterator<String> it = input.iterator();
        if (!it.hasNext()) {
          return "{}";
        }
    
        StringBuilder sb = new StringBuilder(approxSize);
        sb.append('{');
        for (;;) {
          String s = it.next();
          sb.append('"').append(s).append('"');
          if (!it.hasNext()) {
            return sb.append('}').toString();
          }
          sb.append(',');
        }
    
      }
    
    }
