package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.validators.FieldValidator;
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

public class IndLearnerCourseCollectionsPerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerCourseCollectionsPerfHandler.class);
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;
    private double maxScore;

    public IndLearnerCourseCollectionsPerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch Student Performance in Assessments");
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
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {

    	//NOTE: This code will need to be refactored going ahead. (based on changes/updates to Student Performance Reports)

        StringBuilder query = new StringBuilder(AJEntityBaseReports.GET_IL_COURSE_DISTINCT_COLLECTIONS);
        List<String> params = new ArrayList<>();
        JsonObject resultBody = new JsonObject();
        JsonArray collectionArray = new JsonArray();

        String userId = this.context.request().getString(REQUEST_USERID);

      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in a Collection");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);

      } else {
      	params.add(userId);
      }

      params.add(AJEntityBaseReports.ATTR_COLLECTION);

        String classId = this.context.request().getString(MessageConstants.CLASS_ID);
      if(StringUtil.isNullOrEmpty(classId)){
        query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append("class_id IS NULL").append(AJEntityBaseReports.SPACE);
      } else{
        query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.CLASS_ID).append(AJEntityBaseReports.SPACE);
        params.add(classId);
      }

        String courseId = this.context.request().getString(MessageConstants.COURSE_ID);
      if (StringUtil.isNullOrEmpty(courseId)) {
          LOGGER.warn("CourseID is mandatory for fetching Student Performance in a Collection");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("Course Id Missing. Cannot fetch Student Performance in Collection"),
                  ExecutionStatus.FAILED);

        } else {
          query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.COURSE_ID_IS);
        	params.add(courseId);
        }

        String unitId = this.context.request().getString(MessageConstants.UNIT_ID);
      if (!StringUtil.isNullOrEmpty(unitId)) {
    	  query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UNIT_ID_IS);
    	  params.add(unitId);
        }

        String lessonId = this.context.request().getString(MessageConstants.LESSON_ID);
      if (!StringUtil.isNullOrEmpty(lessonId)) {
    	  query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.LESSON_ID_IS);
    	  params.add(lessonId);
        }

      String startDate = this.context.request().getString(MessageConstants.START_DATE);
      if (!StringUtil.isNullOrEmpty(startDate)&&!FieldValidator.validateDate(startDate)) {
        LOGGER.error("Invalid startDate");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid startDate. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);

      }
      if (!StringUtil.isNullOrEmpty(startDate)) {
        query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_GREATER_THAN_OR_EQUAL);
        params.add(startDate);
      }
      String endDate = this.context.request().getString(MessageConstants.END_DATE);
      if (!StringUtil.isNullOrEmpty(endDate)&&!FieldValidator.validateDate(endDate)) {
        LOGGER.error("Invalid endDate");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid endDate. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);

      }
      if (!StringUtil.isNullOrEmpty(endDate)) {
        query.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_LESS_THAN_OR_EQUAL);
        params.add(endDate);
      }
      LOGGER.debug("Query : " + query);
      LazyList<AJEntityBaseReports> collectionList = AJEntityBaseReports.findBySQL(query.toString(), params.toArray());

      //Populate collIds from the Context in API
      List<String> collIds = new ArrayList<>(collectionList.size());
      if (!collectionList.isEmpty()) {
          collectionList.forEach(c -> collIds.add(c.getString(AJEntityBaseReports.COLLECTION_OID)));
      }

        for (String collId : collIds) {
          LazyList<AJEntityBaseReports> collTSA;
        	JsonObject collectionKpi = new JsonObject();
          List<String> collTSAParams = new ArrayList<>();
          collTSAParams.add(courseId);
          collTSAParams.add(collId);
          collTSAParams.add(AJEntityBaseReports.ATTR_COLLECTION);
          collTSAParams.add(userId);

          StringBuilder collTSAQuery = new StringBuilder(AJEntityBaseReports.GET_IL_COURSE_COLLECTION_PERF);
          if (!StringUtil.isNullOrEmpty(startDate)) {
            collTSAQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_GREATER_THAN_OR_EQUAL);
            collTSAParams.add(startDate);
          }
          if (!StringUtil.isNullOrEmpty(endDate)) {
            collTSAQuery.append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UPDATDED_AT_LESS_THAN_OR_EQUAL);
            collTSAParams.add(endDate);
          }
          collTSAQuery.append(" GROUP BY collection_id");
          LOGGER.debug("collTSAQuery : " + collTSAQuery);
        	//Find Timespent and Attempts
        	collTSA = AJEntityBaseReports.findBySQL(collTSAQuery.toString(),collTSAParams.toArray());

        	if (!collTSA.isEmpty()) {
        	collTSA.forEach(m -> {
        		collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.TIME_SPENT).toString()));
        		collectionKpi.put(AJEntityBaseReports.VIEWS, Integer.parseInt(m.get(AJEntityBaseReports.VIEWS).toString()));
	    		});
        	}
          List<Map> collectionMaximumScore;
          collectionMaximumScore = Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_MAX_SCORE, courseId,
                  collId, userId);

          //If questions are not present then Question Count is always zero, however this additional check needs to be added
          //since during migration of data from 3.0 chances are that QC may be null instead of zero
          collectionMaximumScore.forEach(ms -> {
            if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
              this.maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
            } else {
              this.maxScore = 0;
            }
          });
          String latestSessionId = null;
          double scoreInPercent = 0;
          if (this.maxScore > 0) {
            List<Map> collectionScoreList = null;
            if(!StringUtil.isNullOrEmpty(classId)){
              collectionScoreList = Base.findAll(AJEntityBaseReports.GET_COLLECTION_SCORE, classId, courseId,
                      collId, userId);
            } else{
              collectionScoreList = Base.findAll(AJEntityBaseReports.GET_IL_COLLECTION_SCORE,  courseId,
                      collId, userId);
            }
            
            if (collectionScoreList != null && !collectionScoreList.isEmpty()) {
                Map collectionMap = collectionScoreList.get(0);
                if (collectionMap.get(JsonConstants.SCORE) != null) {
                    Double score = Double.valueOf(collectionMap.get(JsonConstants.SCORE).toString());
                    scoreInPercent = ((score / this.maxScore) * 100);
                    collectionKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(scoreInPercent));
                }
                if (collectionMap.get(AJEntityBaseReports.SESSION_ID) != null) latestSessionId = collectionMap.get(AJEntityBaseReports.SESSION_ID) != null ? collectionMap.get(AJEntityBaseReports.SESSION_ID).toString() : null;
            } else {
              collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE); 
            }
          } else {
            //If Collections have No Questions then score should be NULL
            collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
          }

            //As per the current philosophy in Gooru (3.0/4.0) as soon as a collection is viewed, it is assumed to be completed.
            //So whenever we have an entry of a collection_id in Analytics, it is by default complete.
            collectionKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
        	collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
        	
            String gradeStatus = JsonConstants.COMPLETE;
            //Check grading completion with latest session id
            if (latestSessionId != null) {
                List<Map> inprogressListOfGradeStatus = Base.findAll(AJEntityBaseReports.FETCH_INPROGRESS_GRADE_STATUS_BY_SESSION_ID, userId, latestSessionId, collId);
                if (inprogressListOfGradeStatus != null && !inprogressListOfGradeStatus.isEmpty()) gradeStatus = JsonConstants.IN_PROGRESS;
            }
            collectionKpi.put(AJEntityBaseReports.ATTR_GRADE_STATUS, gradeStatus);
            
        	collectionArray.add(collectionKpi);
        	}

        resultBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERID, userId);

      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

      }

    @Override
    public boolean handlerReadOnly() {
      return true;
    }

}
