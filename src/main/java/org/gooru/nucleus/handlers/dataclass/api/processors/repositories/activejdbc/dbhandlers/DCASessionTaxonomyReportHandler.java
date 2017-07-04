package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;

import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDCACompetencyReport;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DCASessionTaxonomyReportHandler implements DBHandler {


	  private static final Logger LOGGER = LoggerFactory.getLogger(DCASessionTaxonomyReportHandler.class);

	  private final ProcessorContext context;

	  DCASessionTaxonomyReportHandler(ProcessorContext context) {
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
	    LOGGER.debug("validateRequest() OK");
	    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	  }

	  @Override
	  @SuppressWarnings("rawtypes")
	  public ExecutionResult<MessageResponse> executeRequest() {
	    JsonArray taxonomyKpiArray = new JsonArray();
	    JsonObject result = new JsonObject();
	    List<Map> sessionTaxonomyResults = Base.findAll(AJEntityDCACompetencyReport.SELECT_DCA_REPORT_IDS, this.context.sessionId());
	    if (!sessionTaxonomyResults.isEmpty()) {
	      sessionTaxonomyResults.forEach(taxonomyRow -> {
	        // Generate aggregate data object
	        JsonObject aggResult = new JsonObject();
	        aggResult.put(JsonConstants.DISPLAY_CODE, taxonomyRow.get(AJEntityDCACompetencyReport.DISPLAY_CODE));

	        if (taxonomyRow.get(AJEntityDCACompetencyReport.TAX_MICRO_STANDARD_ID) != null) {
	          aggResult.put(JsonConstants.LEARNING_TARGET_ID, taxonomyRow.get(AJEntityDCACompetencyReport.TAX_MICRO_STANDARD_ID));
	        } else {
	          aggResult.put(JsonConstants.STANDARDS_ID, taxonomyRow.get(AJEntityDCACompetencyReport.TAX_STANDARD_ID));
	        }

	        LOGGER.debug("Base Reports IDS : {}", listToPostgresArrayInteger(taxonomyRow.get(AJEntityDCACompetencyReport.BASE_REPORT_ID).toString()));
	        List<Map> aggTaxonomyResults = Base.findAll(AJEntityDCACompetencyReport.GET_DCA_AGG_TAX_DATA,
	                listToPostgresArrayInteger(taxonomyRow.get(AJEntityDCACompetencyReport.BASE_REPORT_ID).toString()));

	        if (!aggTaxonomyResults.isEmpty()) {
	          aggTaxonomyResults.stream().forEach(aggData -> {
	            aggResult.put(JsonConstants.TIMESPENT, Long.valueOf(aggData.get(AJEntityDailyClassActivity.TIMESPENT).toString()));
	            aggResult.put(JsonConstants.REACTION, Integer.valueOf(aggData.get(AJEntityDailyClassActivity.REACTION).toString()));
	            aggResult.put(JsonConstants.SCORE, Math.round(Double.valueOf(aggData.get(AJEntityDailyClassActivity.SCORE).toString())));

	          });
	        }
	        List<Map> sessionTaxonomyQuestionResults = Base.findAll(AJEntityDCACompetencyReport.GET_DCA_QUESTIONS_TAX_PERF,
	                listToPostgresArrayInteger(taxonomyRow.get(AJEntityDCACompetencyReport.BASE_REPORT_ID).toString()));

	        // Generate questions array
	        if (!sessionTaxonomyQuestionResults.isEmpty()) {
	          JsonArray questionsArray = new JsonArray();
	          sessionTaxonomyQuestionResults.stream().forEach(question -> {
	            JsonObject questionData = ValueMapper.map(ResponseAttributeIdentifier.getSessionTaxReportQuestionAttributesMap(), question);
	            questionData.put(JsonConstants.SCORE, Math.round(Double.valueOf(question.get(AJEntityDailyClassActivity.SCORE).toString())));
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
	      StringBuilder sb = new StringBuilder(approxSize);
	      sb.append('{');
	      sb.append(in);
	      return sb.append('}').toString();
	    }
	  }

	  @Override
	  public boolean handlerReadOnly() {	    
	    return true;
	  }
}
