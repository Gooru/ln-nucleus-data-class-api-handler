package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntitySessionTaxonomyReport;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SessionTaxonomyReportHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTaxonomyReportHandler.class);

  private final ProcessorContext context;
  private AJEntitySessionTaxonomyReport sessionTaxonomyReport;

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
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    sessionTaxonomyReport = new AJEntitySessionTaxonomyReport();
    JsonArray taxonomyKpiArray = new JsonArray();
    JsonObject result = new JsonObject();
    List<Map> sessionTaxonomyResults =
            Base.findAll(sessionTaxonomyReport.SELECT_TAXONOMY_REPORT_BY_STANDARDS_AND_MICRO_STANDARDS, this.context.sessionId());
    if (!sessionTaxonomyResults.isEmpty()) {
      sessionTaxonomyResults.forEach(taxonomyRow -> {
        JsonObject aggResult = new JsonObject();
        aggResult.put("standardsId", taxonomyRow.get("standard_id"));
        if (taxonomyRow.get("learning_target_id") != null && !taxonomyRow.get("learning_target_id").toString().isEmpty()) {
          aggResult.put("learningTargetId", taxonomyRow.get("learning_target_id"));
        }
        aggResult.put("displayCode", taxonomyRow.get("display_code"));
        aggResult.put("timespent", taxonomyRow.get("agg_time_spent").toString());
        aggResult.put("score", taxonomyRow.get("agg_score").toString());
        JsonObject questions = new JsonObject();
        questions.put("questionId", taxonomyRow.get("resource_id"));
        questions.put("timespent", taxonomyRow.get("time_spent").toString());
        questions.put("attempts", taxonomyRow.get("views").toString());
        questions.put("score", taxonomyRow.get("score").toString());
        questions.put("questionType", taxonomyRow.get("question_type"));
        questions.put("answerStatus", taxonomyRow.get("resource_attempt_status"));
        questions.put("reaction", taxonomyRow.get("reaction").toString());
        if (aggResult.getJsonArray("questions") == null) {
          aggResult.put("questions", new JsonArray().add(questions));
        } else {
          aggResult.put("questions", aggResult.getJsonArray("questions").add(questions));
        }
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
