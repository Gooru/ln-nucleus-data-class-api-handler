package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
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

public class IndLearnerAllIndCollectionPerfHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IndLearnerAllIndCollectionPerfHandler.class);

	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";
	  private String userId;
	  
	  IndLearnerAllIndCollectionPerfHandler(ProcessorContext context) {
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
	    // FIXME :: Teacher validation to be added.
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
      this.userId = this.context.request().getString(REQUEST_USERID);
      String limitS = this.context.request().getString("limit");
      String offsetS = this.context.request().getString("offset");
  
      if (StringUtil.isNullOrEmpty(this.userId)) {
        // If user id is not present in the path, take user id from session token.
        this.userId = this.context.userIdFromSession();
      }
  
      JsonObject result = new JsonObject();
      JsonArray collectionKpiArray = new JsonArray();
      String query = StringUtil.isNullOrEmpty(limitS) ? AJEntityBaseReports.GET_IL_ALL_COLLECTION_VIEWS_TIMESPENT
              : AJEntityBaseReports.GET_IL_ALL_COLLECTION_VIEWS_TIMESPENT + "LIMIT " + Long.valueOf(limitS);
  
      List<Map> collectionAggData = Base.findAll(query, this.userId, StringUtil.isNullOrEmpty(offsetS) ? 0L : Long.valueOf(offsetS));
      if (!collectionAggData.isEmpty()) {
        collectionAggData.forEach(collectionsKpi -> {
          JsonObject collectionObj = new JsonObject();
          collectionObj.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
          collectionObj.put(AJEntityBaseReports.ATTR_TIME_SPENT, Long.parseLong(collectionsKpi.get(AJEntityBaseReports.TIME_SPENT).toString()));
          collectionObj.put(AJEntityBaseReports.VIEWS, Long.parseLong(collectionsKpi.get(AJEntityBaseReports.VIEWS).toString()));
          Object title = Base.firstCell(AJEntityContent.GET_TITLE, collectionsKpi.get(AJEntityBaseReports.COLLECTION_OID).toString());
          collectionObj.put(JsonConstants.COLLECTION_TITLE, title);
          collectionKpiArray.add(collectionObj);
        });
      } else {
        LOGGER.info("No data returned for independant learner all collections performance");
      }
      // Form the required Json pass it on
      result.put(JsonConstants.USAGE_DATA, collectionKpiArray);
  
      result.put(JsonConstants.USERID, this.userId);
  
      return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
    }

	  @Override
	  public boolean handlerReadOnly() {
	    return false;
	  }

}
