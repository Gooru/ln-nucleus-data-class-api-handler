package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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

public class StudPerfMultipleCollectionHandler implements DBHandler{

	private static final Logger LOGGER = LoggerFactory.getLogger(StudPerfMultipleCollectionHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userId";
    private final ProcessorContext context;
    private String collectionType;

    public StudPerfMultipleCollectionHandler(ProcessorContext context) {
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

        JsonObject resultBody = new JsonObject();
        JsonArray collectionArray = new JsonArray();

        String userId = this.context.request().getString(REQUEST_USERID);
        JsonArray collectionIds = this.context.request().getJsonArray(MessageConstants.COLLECTION_IDS);
        LOGGER.debug("userId : {} - collectionIds:{}", userId, collectionIds);

        if (collectionIds.isEmpty()) {
          LOGGER.warn("CollectionIds are mandatory to fetch Student Performance in Assessments");
          return new ExecutionResult<>(
                  MessageResponseFactory.createInvalidRequestResponse("CollectionIds are Missing. Cannot fetch Student Performance for Assessments"),
                  ExecutionStatus.FAILED);
        }

      List<Map> collectionPerf;

      userId = this.context.request().getString(REQUEST_USERID);

      if (StringUtil.isNullOrEmpty(userId)) {
        LOGGER.warn("UserID is mandatory for fetching Student Performance in an Assessment");
        return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("User Id Missing. Cannot fetch Student Performance in Assessment"),
                ExecutionStatus.FAILED);

      }

      LOGGER.debug("UID is " + userId);

      //Populate collIds from the Context in API
      List<String> collIds = new ArrayList<>(collectionIds.size());
      for (Object s : collectionIds) {
          collIds.add(s.toString());
        }

        String classId = this.context.request().getString(MessageConstants.CLASS_ID);
      if (!StringUtil.isNullOrEmpty(classId)) {
        LOGGER.debug("Fetching Performance for Assessments in Class");
        collectionPerf = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_CLASS_COLLECTION, classId,
                listToPostgresArrayString(collIds), AJEntityBaseReports.ATTR_COLLECTION, userId);
      } else {
          LOGGER.debug("Fetching Performance for Assessments outside Class");
          collectionPerf = Base.findAll(AJEntityBaseReports.GET_PERFORMANCE_FOR_COLLECTION,
                  listToPostgresArrayString(collIds), AJEntityBaseReports.ATTR_COLLECTION, userId);

      }

      if (!collectionPerf.isEmpty()) {
          collectionPerf.forEach(m -> {
        	JsonObject collectionKpi = new JsonObject();
      		collectionKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, m.get(AJEntityBaseReports.ATTR_COLLECTION_ID).toString());
        	collectionKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, m.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString());
      		collectionKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, m.get(AJEntityBaseReports.ATTR_ATTEMPTS).toString());

    		collectionArray.add(collectionKpi);

      });


      } else {
    	  LOGGER.debug("No data available for ANY of the Collections passed on to this endpoint");
      }

      /**  for (String collId : collIds) {
        	List<Map> collTSA = null;
        	LOGGER.debug("The collectionIds are" + collId);
        	JsonObject collectionKpi = new JsonObject();

        	//Find Timespent and Attempts
        	collTSA = Base.findAll(AJEntityBaseReports.GET_TOTAL_TIMESPENT_ATTEMPTS_FOR_COLLECTION,
        			collId, AJEntityBaseReports.ATTR_COLLECTION, this.userId, EventConstants.COLLECTION_PLAY);

        	if (!collTSA.isEmpty()) {
        	collTSA.forEach(m -> {
        		collectionKpi.put(AJEntityBaseReports.ATTR_TIMESPENT, m.get(AJEntityBaseReports.ATTR_TIMESPENT).toString());
        		collectionKpi.put(AJEntityBaseReports.VIEWS, m.get(AJEntityBaseReports.VIEWS).toString());
	    		});
        	}

        	collectionKpi.put(AJEntityBaseReports.COLLECTION_OID, collId);
        	collectionArray.add(collectionKpi);
        	} **/

        resultBody.put(JsonConstants.USAGE_DATA, collectionArray).put(JsonConstants.USERUID, userId);
        //resultBody.put("PERF", "WORK IN PROGRESS");

      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

      }


    @Override
    public boolean handlerReadOnly() {
      return true;
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
