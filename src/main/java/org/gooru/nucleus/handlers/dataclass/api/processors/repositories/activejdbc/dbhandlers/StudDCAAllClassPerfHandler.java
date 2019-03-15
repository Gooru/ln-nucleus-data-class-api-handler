package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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

/**
 * @author mukul@gooru
 * 
 */
public class StudDCAAllClassPerfHandler implements DBHandler {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudDCAAllClassPerfHandler.class);
	  private final ProcessorContext context;
	  private static final String REQUEST_USERID = "userId";

		private String userId;
		private List<String> classIds = new ArrayList<>();

	  StudDCAAllClassPerfHandler(ProcessorContext context) {
	    this.context = context;
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> checkSanity() {
			this.userId = this.context.request().getString(REQUEST_USERID);
			
	    JsonArray classes = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
	    LOGGER.debug("userId : {} - classIds:{}", this.userId, classes);
	    
	    if (classes.isEmpty()) {
	      LOGGER.warn("ClassIds are mandatory to fetch Student Performance in Classess");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("ClassIds missing. Cannot fetch performance in classes"),
	              ExecutionStatus.FAILED);
	    }
	    
	    for (Object s : classes) {
	    	this.classIds.add(s.toString());
			}
			
			LOGGER.debug("checkSanity() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> validateRequest() {
			if (context.getUserIdFromRequest() == null
						|| (context.getUserIdFromRequest() != null 
						&& !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {

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
	    JsonObject result = new JsonObject();
	    JsonArray classKpiArray;

	    if (!StringUtil.isNullOrEmpty(this.userId)) {
				//
				// Student access to Class Data
				//
				classKpiArray = fetchClassKPIForStudent();
			} else {
				//
				// Teacher access to Class Data
				//
				classKpiArray = fetchClassKPIForTeacher();
			}

	    result.put(JsonConstants.USAGE_DATA, classKpiArray);
	    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
	  }

	  @Override
	  public boolean handlerReadOnly() {
	    return true;
		}
		
		private JsonArray fetchClassKPIForTeacher() {
			JsonArray classKpiArray = new JsonArray();
			for (String clId : this.classIds) {
				JsonObject classKPI = new JsonObject();
				classKPI.put(AJEntityDailyClassActivity.ATTR_CLASS_ID, clId);

				// Average performance across All attempts of Assessments / Ext-Assessments for class
				List<Map> classPerfList = Base.findAll(AJEntityDailyClassActivity.SELECT_CLASS_COMPLETION_SCORE_FOR_TEACHER, clId);
				if (classPerfList != null && !classPerfList.isEmpty()) {
					// only one record expected...but the findAll returns a list...use get(0)
					classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT, 
								classPerfList.get(0).get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT) == null 
								? 0 : Integer.valueOf(classPerfList.get(0).get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT).toString()));

					classKPI.put(AJEntityDailyClassActivity.ATTR_SCORE, 
								classPerfList.get(0).get(AJEntityDailyClassActivity.ATTR_SCORE) == null 
								? null : Math.round(Double.valueOf(classPerfList.get(0).get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
				} else {
					classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT, 0);
					classKPI.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
				}

				classKpiArray.add(classKPI);
			}

			return classKpiArray;
		}

		private JsonArray fetchClassKPIForStudent() {
			JsonArray classKpiArray = new JsonArray();
			for (String clId : this.classIds) {
				JsonObject classKPI = new JsonObject();
				classKPI.put(AJEntityDailyClassActivity.ATTR_CLASS_ID, clId);

				// Average performance across All attempts of Assessments / Ext-Assessments
				List<Map> classScoreCompletion = Base.findAll(AJEntityDailyClassActivity.SELECT_STUDENT_CLASS_COMPLETION_SCORE, clId, this.userId);
				if (classScoreCompletion != null && !classScoreCompletion.isEmpty()) {
					// only one record expected...but the findAll returns a list...use get(0)
					classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT,
									classScoreCompletion.get(0).get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT) == null 
									? 0 : Integer.valueOf(classScoreCompletion.get(0).get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT).toString()));

					classKPI.put(AJEntityDailyClassActivity.ATTR_SCORE, 
								classScoreCompletion.get(0).get(AJEntityDailyClassActivity.ATTR_SCORE) == null 
								? null : Math.round(Double.valueOf(classScoreCompletion.get(0).get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
				} else {
						classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT, 0);
						classKPI.putNull(AJEntityDailyClassActivity.ATTR_SCORE);
				}

				classKpiArray.add(classKPI);	    		
			}

			return classKpiArray;
		}

}