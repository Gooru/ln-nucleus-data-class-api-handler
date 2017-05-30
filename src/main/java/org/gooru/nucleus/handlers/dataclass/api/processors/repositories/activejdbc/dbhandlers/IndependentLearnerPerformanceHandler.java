package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
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

public class IndependentLearnerPerformanceHandler implements DBHandler {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IndependentLearnerPerformanceHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private static final String REQUEST_CONTENTTYPE = "contentType";
	  private static final int MAX_LIMIT = 20;
	  private String userId;
	  int questionCount;
	  
	  IndependentLearnerPerformanceHandler(ProcessorContext context) {
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
	  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    this.userId = this.context.request().getString(REQUEST_USERID);
    if (StringUtil.isNullOrEmpty(this.userId)) {
        // If user id is not present in the path, take user id from session token.
        this.userId = this.context.userIdFromSession();
      }
    String limitS = this.context.request().getString("limit");
    String offsetS = this.context.request().getString("offset");
    
    JsonObject result = new JsonObject();
    JsonArray ILPerfArray = new JsonArray();
    
    String contentType = this.context.request().getString(REQUEST_CONTENTTYPE);
	  
    if (StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.COURSE)){
    	
        String query = StringUtil.isNullOrEmpty(limitS) ? AJEntityBaseReports.GET_IL_ALL_COURSE_TIMESPENT
                : AJEntityBaseReports.GET_IL_ALL_COURSE_TIMESPENT + "LIMIT " + Long.valueOf(limitS);

        List<Map> courseTSKpi = Base.findAll(query, this.userId, StringUtil.isNullOrEmpty(offsetS) ? 0L : Long.valueOf(offsetS));
        if (!courseTSKpi.isEmpty()) {
          courseTSKpi.forEach(courseTS -> {
            JsonObject courseDataObject = new JsonObject();
            courseDataObject.put(AJEntityBaseReports.ATTR_COURSE_ID, courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
            Object title = Base.firstCell(AJEntityContent.GET_TITLE, courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
            courseDataObject.put(JsonConstants.COURSE_TITLE, title);
            courseDataObject.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(courseTS.get(AJEntityBaseReports.TIME_SPENT).toString()));
            List<Map> courseCompletionKpi = Base.findAll(AJEntityBaseReports.GET_IL_ALL_COURSE_SCORE_COMPLETION, this.userId,
                    courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
            courseCompletionKpi.forEach(courseComplettion -> {
              courseDataObject.put(AJEntityBaseReports.ATTR_SCORE,
                      Math.round(Double.valueOf(courseComplettion.get(AJEntityBaseReports.ATTR_SCORE).toString())));
              courseDataObject.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                      Integer.parseInt(courseComplettion.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
            });
            Object courseTotalAssCount = Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT,
                    courseTS.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
            courseDataObject.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, courseTotalAssCount != null ? Integer.valueOf(courseTotalAssCount.toString()) : 0);
            courseDataObject.putNull(AJEntityBaseReports.ATTR_COLLECTION_ID);
            courseDataObject.putNull(JsonConstants.COLLECTION_TITLE);
            courseDataObject.putNull(AJEntityBaseReports.ATTR_ATTEMPTS);
            ILPerfArray.add(courseDataObject);                        
          });
        } else {
          LOGGER.info("No data returned for Independent Learner for All Courses");
        }
    	
    } else if (StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.ASSESSMENT)) {
    	
        String query = StringUtil.isNullOrEmpty(limitS) ? AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_ATTEMPTS_TIMESPENT
                : AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_ATTEMPTS_TIMESPENT + "LIMIT " + Long.valueOf(limitS);
    
        List<Map> assessmentTS = Base.findAll(query, this.userId, StringUtil.isNullOrEmpty(offsetS) ? 0L : Long.valueOf(offsetS));
        if (!assessmentTS.isEmpty()) {
          assessmentTS.forEach(assessmentTsKpi -> {
            JsonObject assesmentObject = new JsonObject();
            assesmentObject.put(AJEntityBaseReports.ATTR_COLLECTION_ID, assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
            assesmentObject.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(assessmentTsKpi.get(AJEntityBaseReports.TIME_SPENT).toString()));
            assesmentObject.put(AJEntityBaseReports.ATTR_ATTEMPTS, Long.parseLong(assessmentTsKpi.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString()));
            List<Map> assessmentCompletionKpi = Base.findAll(AJEntityBaseReports.GET_IL_ALL_ASSESSMENT_SCORE_COMPLETION, this.userId,
                    assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
            if (!assessmentCompletionKpi.isEmpty()) {
              assessmentCompletionKpi.forEach(courseComplettion -> {
                assesmentObject.put(AJEntityBaseReports.ATTR_SCORE,
                        Math.round(Double.valueOf(courseComplettion.get(AJEntityBaseReports.ATTR_SCORE).toString())));
              });
            } else {
              assesmentObject.put(AJEntityBaseReports.ATTR_SCORE, 0);
            }
            Object title = Base.firstCell(AJEntityContent.GET_TITLE, assessmentTsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
            assesmentObject.put(JsonConstants.COLLECTION_TITLE, title);
            assesmentObject.putNull(AJEntityBaseReports.ATTR_COURSE_ID);
            assesmentObject.putNull(JsonConstants.COURSE_TITLE);
            assesmentObject.putNull(AJEntityBaseReports.ATTR_COMPLETED_COUNT);
            assesmentObject.putNull(AJEntityBaseReports.ATTR_TOTAL_COUNT);
            ILPerfArray.add(assesmentObject);
          });
        } else {
          LOGGER.info("No data returned for Independent Learner for Standalone Assessments");
        }

    	
    } else if (StringUtil.isNullOrEmpty(contentType) && contentType.equalsIgnoreCase(MessageConstants.COLLECTION)) {    	
    	
        String query = StringUtil.isNullOrEmpty(limitS) ? AJEntityBaseReports.GET_IL_ALL_COLLECTION_VIEWS_TIMESPENT
                : AJEntityBaseReports.GET_IL_ALL_COLLECTION_VIEWS_TIMESPENT + "LIMIT " + Long.valueOf(limitS);
    
        List<Map> collectionAggData = Base.findAll(query, this.userId, StringUtil.isNullOrEmpty(offsetS) ? 0L : Long.valueOf(offsetS));
        if (!collectionAggData.isEmpty()) {
          collectionAggData.forEach(collectionsKpi -> {
            JsonObject collectionObj = new JsonObject();
            String collId = collectionsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString();
            collectionObj.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId );
            collectionObj.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(collectionsKpi.get(AJEntityBaseReports.TIME_SPENT).toString()));
            collectionObj.put(AJEntityBaseReports.VIEWS, Long.parseLong(collectionsKpi.get(AJEntityBaseReports.VIEWS).toString()));
            
            List<Map> collectionQuestionCount = null;
            collectionQuestionCount = Base.findAll(AJEntityBaseReports.GET_COLLECTION_QUESTION_COUNT, null, null,
                    collId, this.userId);
            collectionQuestionCount.forEach(qc -> {
                questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
              });

            double scoreInPercent = 0;
            if (questionCount > 0) {
              Object collectionScore = null;
              collectionScore = Base.firstCell(AJEntityBaseReports.GET_COLLECTION_SCORE, null, null,
                      collId, this.userId);
              if (collectionScore != null) {
                scoreInPercent = (((Double.valueOf(collectionScore.toString())) / questionCount) * 100);
              }
            }
            collectionObj.put(AJEntityBaseReports.ATTR_SCORE, Math.round(scoreInPercent));
            Object title = Base.firstCell(AJEntityContent.GET_TITLE, collectionsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
            collectionObj.put(JsonConstants.COLLECTION_TITLE, title);
            collectionObj.putNull(AJEntityBaseReports.ATTR_COURSE_ID);
            collectionObj.putNull(JsonConstants.COURSE_TITLE);
            collectionObj.putNull(AJEntityBaseReports.ATTR_COMPLETED_COUNT);
            collectionObj.putNull(AJEntityBaseReports.ATTR_TOTAL_COUNT);
            ILPerfArray.add(collectionObj);
          });
        } else {
          LOGGER.info("No data returned for Independent Learner for Standalone Collections");
        }    	
    }
    
    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, ILPerfArray).put(JsonConstants.USERID, this.userId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
	  }
	}
