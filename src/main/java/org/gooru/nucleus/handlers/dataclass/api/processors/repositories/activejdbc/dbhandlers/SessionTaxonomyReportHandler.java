package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntitySessionTaxonomyReport;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueTypeCaster;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SessionTaxonomyReportHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTaxonomyReportHandler.class);

  private final ProcessorContext context;
  private AJEntitySessionTaxonomyReport sessionTaxonomyReport;
  private AJEntityClassAuthorizedUsers classAuthorizedUsers;

  SessionTaxonomyReportHandler(ProcessorContext context) {
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
   /* classAuthorizedUsers = new AJEntityClassAuthorizedUsers();
    List<Map> creator = Base.findAll(classAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userId());
    if (creator.isEmpty()) {
      List<Map> collaborator = Base.findAll(classAuthorizedUsers.SELECT_CLASS_CREATOR, this.context.classId(), this.context.userId());
      if (collaborator.isEmpty()) {
        return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
      }
    }*/
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    sessionTaxonomyReport = new AJEntitySessionTaxonomyReport();
    JsonArray taxonomyKpiArray = new JsonArray();
    JsonObject result = new JsonObject();
    List<Map> sessionTaxonomyResults = Base.findAll(sessionTaxonomyReport.SELECT_TAXONOMY_REPORT_AGG_METRICS, this.context.sessionId());
    if (!sessionTaxonomyResults.isEmpty()) {
      sessionTaxonomyResults.forEach(taxonomyRow -> {
        JsonObject aggResult = new JsonObject();
        List<Map> sessionTaxonomyQuestionResults = null;
        if (taxonomyRow.get("learning_target_id") != null && !taxonomyRow.get("learning_target_id").toString().isEmpty()) {
          aggResult.put("learningTargetId", taxonomyRow.get("learning_target_id"));
          sessionTaxonomyQuestionResults = Base.findAll(sessionTaxonomyReport.SELECT_TAXONOMY_REPORT_BY_MICRO_STANDARDS, this.context.sessionId(),taxonomyRow.get("subject_id"),taxonomyRow.get("course_id"),taxonomyRow.get("domain_id"),taxonomyRow.get("standard_id"),taxonomyRow.get("learning_target_id"));
        } else {
          aggResult.put("standardsId", taxonomyRow.get("standard_id"));
          sessionTaxonomyQuestionResults = Base.findAll(sessionTaxonomyReport.SELECT_TAXONOMY_REPORT_BY_STANDARDS, this.context.sessionId(),taxonomyRow.get("subject_id"),taxonomyRow.get("course_id"),taxonomyRow.get("domain_id"),taxonomyRow.get("standard_id"));
        }
        aggResult.put("displayCode", taxonomyRow.get("display_code"));
        aggResult.put("timespent", ValueTypeCaster.castValue(taxonomyRow.get("time_spent")));
        aggResult.put("score", ValueTypeCaster.castValue(taxonomyRow.get("score")));

        JsonArray questionsArray = new JsonArray();
        sessionTaxonomyQuestionResults.forEach(questionRows -> {
          JsonObject question = new JsonObject();
          question.put("questionId", questionRows.get("resource_id"));
          question.put("timespent", ValueTypeCaster.castValue(questionRows.get("time_spent")));
          question.put("attempts", ValueTypeCaster.castValue(questionRows.get("views")));
          question.put("score", ValueTypeCaster.castValue(questionRows.get("score")));
          question.put("questionType", questionRows.get("question_type"));
          question.put("answerStatus", questionRows.get("resource_attempt_status"));
          question.put("reaction", ValueTypeCaster.castValue(questionRows.get("reaction")));
          questionsArray.add(question);
        });

        aggResult.put("questions", questionsArray);
        taxonomyKpiArray.add(aggResult);
      });
    } else {
      LOGGER.info("No Records found for given session ID");
      return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
    }

    result.put("content", taxonomyKpiArray);
    LOGGER.info(result.encodePrettily());

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

}
