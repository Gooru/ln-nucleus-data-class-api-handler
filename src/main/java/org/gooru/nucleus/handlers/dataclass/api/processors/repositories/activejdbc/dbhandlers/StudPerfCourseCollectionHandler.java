package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

public class StudPerfCourseCollectionHandler implements DBHandler {


	private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfCourseCollectionHandler.class);
    private static final String REQUEST_USERID = "userId";
    private static final String COLLECTION = "collection";
    private final ProcessorContext context;
    private double maxScore;

    public StudPerfCourseCollectionHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid request received to fetch Student Performance in Collections");
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
              || (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.request().getString(MessageConstants.CLASS_ID), this.context.userIdFromSession());
        if (owner.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
        }
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ExecutionResult<MessageResponse> executeRequest() {
        StringBuilder query = new StringBuilder(AJEntityBaseReports.GET_DISTINCT_COLLECTIONS_BULK);
        List<String> params = new ArrayList<>();
        JsonObject resultBody = new JsonObject();                
        JsonArray resultArray = new JsonArray();
        String userId = this.context.request().getString(REQUEST_USERID);

      params.add(AJEntityBaseReports.ATTR_COLLECTION);
      
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
      
      String userId1 = this.context.request().getString(REQUEST_USERID);
      query.append(AJEntityBaseReports.AND).append(AJEntityBaseReports.SPACE).append(AJEntityBaseReports.ACTOR_ID_IS);
      List<String> userIds;
      if (StringUtil.isNullOrEmpty(userId1)) {
        LOGGER.warn("UserID is not in the request to fetch Student Performance in Course. Asseume user is a teacher");
        LazyList<AJEntityBaseReports> userIdOfClass =
      		  AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_COURSE_ID_FILTERBY_COLLTYPE, 
      				  classId, courseId, COLLECTION );
        userIds = userIdOfClass.collect(AJEntityBaseReports.GOORUUID);
      } else {
        userIds = new ArrayList<>(1);
        userIds.add(userId1);
      }

      for (String userID : userIds) {
    	  JsonObject contentBody = new JsonObject();
    	  JsonArray collectionArray = new JsonArray();
    	  
		  //Add user Id to the query    	      
    	  params.add(userID);
          LazyList<AJEntityBaseReports> collectionList = AJEntityBaseReports.findBySQL(query.toString(), params.toArray());

          //Populate collIds from the Context in API
          List<String> collIds = new ArrayList<>(collectionList.size());
          if (!collectionList.isEmpty()) {
              collectionList.forEach(c -> collIds.add(c.getString(AJEntityBaseReports.COLLECTION_OID)));
          }

            for (String collId : collIds) {
            	List<Map> collTSA;
            	JsonObject collectionKpi = new JsonObject();

            	//Find Timespent and Attempts
            	collTSA = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_COLLECTION, classId, courseId,
            			collId, AJEntityBaseReports.ATTR_COLLECTION, userID);

            	if (!collTSA.isEmpty()) {
            	collTSA.forEach(m -> {
            		collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, m.get(AJEntityBaseReports.ATTR_TIME_SPENT) != null ? 
            				Long.parseLong(m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()) : null);
            		collectionKpi.put(AJEntityBaseReports.VIEWS, m.get(AJEntityBaseReports.VIEWS) != null ? 
            				Integer.parseInt(m.get(AJEntityBaseReports.VIEWS).toString()) : null);
    	    		});
            	}
            	
                List<Map> collectionMaximumScore;
                collectionMaximumScore = Base.findAll(AJEntityBaseReports.GET_COLLECTION_MAX_SCORE, classId, courseId,
                        collId, userID);

                //If questions are not present then Question Count is always zero, however this additional check needs to be added
                //since during migration of data from 3.0 chances are that QC may be null instead of zero
                collectionMaximumScore.forEach(ms -> {
                	if (ms.get(AJEntityBaseReports.MAX_SCORE) != null) {
                		this.maxScore = Double.valueOf(ms.get(AJEntityBaseReports.MAX_SCORE).toString());
                	} else {
                		this.maxScore = 0;
                	}
                });
                double scoreInPercent = 0;
                if (this.maxScore > 0) {
                  Object collectionScore;
                  collectionScore = Base.firstCell(AJEntityBaseReports.GET_COLLECTION_SCORE, classId, courseId,
                          collId, userID);
                  if (collectionScore != null) {
                    scoreInPercent = (((Double.valueOf(collectionScore.toString())) / this.maxScore) * 100);
                    collectionKpi.put(AJEntityBaseReports.ATTR_SCORE, Math.round(scoreInPercent));
                  } else {
                	  collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
                  }              
                } else {
                	//If Collections have No Questions then score should be NULL
                	collectionKpi.putNull(AJEntityBaseReports.ATTR_SCORE);
                }
                collectionKpi.put(JsonConstants.STATUS, JsonConstants.COMPLETE);
                
                collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
                
                //Fetch grading status for each collection
                List<Map> inprogressListOfGradeStatus = Base.findAll(AJEntityBaseReports.FETCH_INPROGRESS_COLL_GRADE_STATUS, classId, courseId,
                    collId, userID);
                String gradeStatus = JsonConstants.IN_PROGRESS;
                if(inprogressListOfGradeStatus == null || inprogressListOfGradeStatus.isEmpty()) gradeStatus = JsonConstants.COMPLETE;
                collectionKpi.put(AJEntityBaseReports.ATTR_GRADE_STATUS, gradeStatus);
                
                collectionArray.add(collectionKpi);
            }

            contentBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERID, userID);
            resultArray.add(contentBody);
            params.remove(userID);    	  
      }
      
      resultBody.put(JsonConstants.CONTENT, resultArray);
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
      }

    @Override
    public boolean handlerReadOnly() {
      return true;
    }

}
