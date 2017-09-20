package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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

public class StudPerfCourseAssessmentHandler implements DBHandler {


	private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfCourseAssessmentHandler.class);
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;

    public StudPerfCourseAssessmentHandler(ProcessorContext context) {
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

    	//NOTE: This code will need to be refactored going ahead. (based on changes/updates to Student Performance Reports)

        StringBuilder query = new StringBuilder(AJEntityBaseReports.GET_DISTINCT_COLLECTIONS);
        List<String> params = new ArrayList<>();
        JsonObject resultBody = new JsonObject();
        JsonArray assessmentArray = new JsonArray();

        String userId = this.context.request().getString(REQUEST_USERID);

      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in a Collection");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Collection"),
                ExecutionStatus.FAILED);

      } else {
      	params.add(userId);
      }

      params.add(AJEntityBaseReports.ATTR_ASSESSMENT);

        String classId = this.context.request().getString(MessageConstants.CLASS_ID);
      if (StringUtil.isNullOrEmpty(classId)) {
          LOGGER.warn("ClassID is mandatory for fetching Student Performance in a Collection");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("Class Id Missing. Cannot fetch Student Performance in Collection"),
                  ExecutionStatus.FAILED);

        } else {
        	params.add(classId);
        }

        String courseId = this.context.request().getString(MessageConstants.COURSE_ID);
      if (StringUtil.isNullOrEmpty(courseId)) {
          LOGGER.warn("CourseID is mandatory for fetching Student Performance in a Collection");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("Course Id Missing. Cannot fetch Student Performance in Collection"),
                  ExecutionStatus.FAILED);

        } else {
        	params.add(courseId);
        }

        String unitId = this.context.request().getString(MessageConstants.UNIT_ID);
      if (!StringUtil.isNullOrEmpty(unitId)) {
    	  query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.UNIT_ID);
    	  params.add(unitId);
        }

        String lessonId = this.context.request().getString(MessageConstants.LESSON_ID);
      if (!StringUtil.isNullOrEmpty(lessonId)) {
    	  query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.LESSON_ID);
    	  params.add(lessonId);
        }

      LOGGER.debug("StudPerfCourseAssessmentHandler Query : {}", query.toString());

      LazyList<AJEntityBaseReports> collectionList = AJEntityBaseReports.findBySQL(query.toString(), params.toArray());

      //Populate collIds from the Context in API
      List<String> collIds = new ArrayList<>(collectionList.size());
      if (!collectionList.isEmpty()) {
          collectionList.forEach(c -> collIds.add(c.getString(AJEntityBaseReports.COLLECTION_OID)));
      }
        for (String collId : collIds) {
        	List<Map> assessScore;
        	List<Map> assessTSA;
        	LOGGER.debug("The collectionIds are" + collId);
        	JsonObject assessmentKpi = new JsonObject();

        	//Find Timespent and Attempts
        	assessTSA = Base.findAll(AJEntityBaseReports.GET_TOTAL_TIME_SPENT_ATTEMPTS_FOR_ASSESSMENT, classId,
                courseId,
        			collId, AJEntityBaseReports.ATTR_ASSESSMENT, AJEntityBaseReports.ATTR_CP_EVENTNAME, userId, EventConstants.STOP);

        	if (!assessTSA.isEmpty()) {
        	assessTSA.forEach(m -> {
        		assessmentKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
        		assessmentKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, Integer.parseInt(m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()));
	    		});
        	}

        	//Get the latest Score
        	assessScore = Base.findAll(AJEntityBaseReports.GET_LATEST_SCORE_FOR_ASSESSMENT, classId, courseId,
            		collId, AJEntityBaseReports.ATTR_ASSESSMENT, userId, EventConstants.COLLECTION_PLAY, EventConstants.STOP);

        	if (!assessScore.isEmpty()){
        		assessScore.forEach(m -> {
            		assessmentKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(Double.valueOf(m.get(AJEntityBaseReports.ATTR_SCORE).toString())));
            		assessmentKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
    	    		});
            	}

        	assessmentKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
        	assessmentArray.add(assessmentKpi);
        	}

      resultBody.put(JsonConstants.USAGE_DATA, assessmentArray).put(JsonConstants.USERID, userId);

      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

      }


    @Override
    public boolean handlerReadOnly() {
      return true;
    }


}
