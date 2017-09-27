package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCompetencyReport;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
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
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonArray taxonomyKpiArray = new JsonArray();
    JsonObject result = new JsonObject();
    List<Map> sessionTaxonomyResults = Base.findAll(AJEntityCompetencyReport.SELECT_BASE_REPORT_IDS, this.context.sessionId());
    if (!sessionTaxonomyResults.isEmpty()) {
      sessionTaxonomyResults.forEach(taxonomyRow -> {
        // Generate aggregate data object
        JsonObject aggResult = new JsonObject();
        aggResult.put(JsonConstants.DISPLAY_CODE, taxonomyRow.get(AJEntityCompetencyReport.DISPLAY_CODE));

        if (taxonomyRow.get(AJEntityCompetencyReport.TAX_MICRO_STANDARD_ID) != null) {
          aggResult.put(JsonConstants.LEARNING_TARGET_ID, taxonomyRow.get(AJEntityCompetencyReport.TAX_MICRO_STANDARD_ID));
        } else {
          aggResult.put(JsonConstants.STANDARDS_ID, taxonomyRow.get(AJEntityCompetencyReport.TAX_STANDARD_ID));
        }

        LOGGER.debug("Base Reports IDS : {}", listToPostgresArrayInteger(taxonomyRow.get(AJEntityCompetencyReport.BASE_REPORT_ID).toString()));
        List<Map> aggTaxonomyResults = Base.findAll(AJEntityCompetencyReport.GET_AGG_TAX_DATA,
                listToPostgresArrayInteger(taxonomyRow.get(AJEntityCompetencyReport.BASE_REPORT_ID).toString()));

        if (!aggTaxonomyResults.isEmpty()) {
          aggTaxonomyResults.forEach(aggData -> {
            aggResult.put(JsonConstants.TIMESPENT, Long.valueOf(aggData.get(AJEntityBaseReports.TIME_SPENT).toString()));
            aggResult.put(JsonConstants.REACTION, Integer.valueOf(aggData.get(AJEntityBaseReports.REACTION).toString()));
            aggResult.put(JsonConstants.SCORE, Math.round(Double.valueOf(aggData.get(AJEntityBaseReports.SCORE).toString())));

          });
        }
        List<Map> sessionTaxonomyQuestionResults = Base.findAll(AJEntityCompetencyReport.GET_QUESTIONS_TAX_PERF,
                listToPostgresArrayInteger(taxonomyRow.get(AJEntityCompetencyReport.BASE_REPORT_ID).toString()));

        // Generate questions array
        if (!sessionTaxonomyQuestionResults.isEmpty()) {
          JsonArray questionsArray = new JsonArray();
          sessionTaxonomyQuestionResults.forEach(question -> {
            JsonObject questionData = ValueMapper.map(ResponseAttributeIdentifier.getSessionTaxReportQuestionAttributesMap(), question);
            questionData.put(JsonConstants.SCORE, Math.round(Double.valueOf(question.get(AJEntityBaseReports.SCORE).toString())));
            questionsArray.add(questionData);
          });
          aggResult.put(JsonConstants.QUESTIONS, questionsArray);
          taxonomyKpiArray.add(aggResult);
        }

      });
    } else {
      LOGGER.info("No Records found for given session ID");
    }
    result.put(JsonConstants.CONTENT, taxonomyKpiArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
  }

  private String listToPostgresArrayInteger(String in) {
    if (in == null) {
      return "{}";
    } else {
      int approxSize = (in.length() + 3);
      String sb = "{" + in + '}';
      return sb;
    }
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
