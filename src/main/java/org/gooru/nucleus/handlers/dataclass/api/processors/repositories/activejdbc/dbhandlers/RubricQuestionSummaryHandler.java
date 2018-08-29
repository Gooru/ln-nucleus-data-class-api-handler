package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RubricQuestionSummaryHandler implements DBHandler {

	  private static final Logger LOGGER = LoggerFactory.getLogger(RubricQuestionSummaryHandler.class);

	  private final ProcessorContext context;

	private String classId;
	  private String courseId;
	private String questionId;
	  private String collectionId;
	  private String studentId;

	  public RubricQuestionSummaryHandler(ProcessorContext context) {
	      this.context = context;
	  }

	  @Override
	  public ExecutionResult<MessageResponse> checkSanity() {

	      LOGGER.debug("checkSanity() OK");
	      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> validateRequest() {

	        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
	        if (owner.isEmpty()) {
	          LOGGER.debug("validateRequest() FAILED");
	          return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not authorized for Rubric Grading"), ExecutionStatus.FAILED);
	        }

	        LOGGER.debug("validateRequest() OK");
	        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	public ExecutionResult<MessageResponse> executeRequest() {
	  JsonObject result = new JsonObject();
	  JsonArray resultArray = new JsonArray();

		  AJEntityRubricGrading rubricReport = new AJEntityRubricGrading();

	  this.studentId = this.context.request().getString(MessageConstants.STUDENTID);
	  if (StringUtil.isNullOrEmpty(studentId)) {
	      LOGGER.warn("StudentID is mandatory to fetch Rubric Question Summary");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Student Id Missing. Cannot fetch Rubric Question Summary"),
	              ExecutionStatus.FAILED);
	    }

		  String sessionId = this.context.request().getString(MessageConstants.SESSION_ID);
	  if (StringUtil.isNullOrEmpty(sessionId)) {
	      LOGGER.warn("SessionID is mandatory to fetch Rubric Question Summary");
	      return new ExecutionResult<>(
	              MessageResponseFactory.createInvalidRequestResponse("Question Id Missing. Cannot fetch Rubric Question Summary"),
	              ExecutionStatus.FAILED);
	    }

	    //Currently its assumed that teacher will have only one attempt at grading a student's question, so there should be
	    //only one record per session, per student for this question. (possibly even for collection)
		List<Map> summaryMap = Base.findAll(AJEntityRubricGrading.GET_RUBRIC_GRADE_FOR_QUESTION, context.classId(),
				context.courseId(), context.collectionId(), context.questionId(), this.studentId, sessionId);

		if (!summaryMap.isEmpty()){
			summaryMap.forEach(m -> {
	    JsonObject smry = new JsonObject();
	    smry.put(AJEntityRubricGrading.ATTR_STUDENT_ID, this.studentId);
	    smry.put(AJEntityRubricGrading.ATTR_STUDENT_SCORE, m.get(AJEntityRubricGrading.STUDENT_SCORE) != null
	    		? Math.round(Double.valueOf(m.get(AJEntityRubricGrading.STUDENT_SCORE).toString())) : null);
	    smry.put(AJEntityRubricGrading.ATTR_MAX_SCORE, m.get(AJEntityRubricGrading.MAX_SCORE) != null
	    		? Math.round(Double.valueOf(m.get(AJEntityRubricGrading.MAX_SCORE).toString())) : null);
	    smry.put(AJEntityRubricGrading.ATTR_OVERALL_COMMENT, m.get(AJEntityRubricGrading.OVERALL_COMMENT) != null
	    		? (m.get(AJEntityRubricGrading.OVERALL_COMMENT).toString()) : null);
	    smry.put(AJEntityRubricGrading.ATTR_CATEGORY_SCORE, m.get(AJEntityRubricGrading.CATEGORY_SCORE) != null
	    		? new JsonArray((m.get(AJEntityRubricGrading.CATEGORY_SCORE).toString())) : null);

	    resultArray.add(smry);
	  });

		} else {
	      LOGGER.info("Rubric Question Summary cannot be obtained");
	  }

	  result.put(JsonConstants.QUE_RUBRICS, resultArray);

	  return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

	}

	  @Override
	  public boolean handlerReadOnly() {
	      return true;
	  }


}
