package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntitySessionTaxonomyReport;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.CollectionUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SessionTaxonomyReportHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTaxonomyReportHandler.class);

  private final ProcessorContext context;
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
    /*
     * classAuthorizedUsers = new AJEntityClassAuthorizedUsers(); List<Map>
     * creator = Base.findAll(classAuthorizedUsers.SELECT_CLASS_CREATOR,
     * this.context.classId(), this.context.userId()); if (creator.isEmpty()) {
     * List<Map> collaborator =
     * Base.findAll(classAuthorizedUsers.SELECT_CLASS_CREATOR,
     * this.context.classId(), this.context.userId()); if
     * (collaborator.isEmpty()) { return new
     * ExecutionResult<>(MessageResponseFactory.
     * createForbiddenResponse("User is not a teacher/collaborator"),
     * ExecutionStatus.FAILED); } }
     */
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);

  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonArray taxonomyKpiArray = new JsonArray();
    JsonObject result = new JsonObject();
    List<Map> sessionTaxonomyResults = Base.findAll(AJEntitySessionTaxonomyReport.SELECT_TAXONOMY_REPORT_AGG_METRICS, this.context.sessionId());
    if (!sessionTaxonomyResults.isEmpty()) {
      sessionTaxonomyResults.forEach(taxonomyRow -> {
        // Generate aggregate data object
        JsonObject aggResult = ValueMapper.map(ResponseAttributeIdentifier.getSessionTaxReportAggAttributesMap(), taxonomyRow);
        List<Map> sessionTaxonomyQuestionResults = null;
        if (taxonomyRow.get(AJEntitySessionTaxonomyReport.LEARNING_TARGET_ID) != null) {
          aggResult.put(JsonConstants.LEARNING_TARGET_ID, taxonomyRow.get(AJEntitySessionTaxonomyReport.LEARNING_TARGET_ID));
          sessionTaxonomyQuestionResults = Base.findAll(AJEntitySessionTaxonomyReport.SELECT_TAXONOMY_REPORT_BY_MICRO_STANDARDS,
                  this.context.sessionId(), taxonomyRow.get(AJEntitySessionTaxonomyReport.SUBJECT_ID),
                  taxonomyRow.get(AJEntitySessionTaxonomyReport.COURSE_ID), taxonomyRow.get(AJEntitySessionTaxonomyReport.DOMAIN_ID),
                  taxonomyRow.get(AJEntitySessionTaxonomyReport.STANDARD_ID), taxonomyRow.get(AJEntitySessionTaxonomyReport.LEARNING_TARGET_ID));
        } else {
          aggResult.put(JsonConstants.STANDARDS_ID, taxonomyRow.get(AJEntitySessionTaxonomyReport.STANDARD_ID));
          sessionTaxonomyQuestionResults = Base.findAll(AJEntitySessionTaxonomyReport.SELECT_TAXONOMY_REPORT_BY_STANDARDS, this.context.sessionId(),
                  taxonomyRow.get(AJEntitySessionTaxonomyReport.SUBJECT_ID), taxonomyRow.get(AJEntitySessionTaxonomyReport.COURSE_ID),
                  taxonomyRow.get(AJEntitySessionTaxonomyReport.DOMAIN_ID), taxonomyRow.get(AJEntitySessionTaxonomyReport.STANDARD_ID));
        }
        // Generate questions array
        JsonArray questionsArray =
                ValueMapper.map(ResponseAttributeIdentifier.getSessionTaxReportQuestionAttributesMap(), sessionTaxonomyQuestionResults);
        aggResult.put(JsonConstants.QUESTIONS, questionsArray);
        taxonomyKpiArray.add(aggResult);
      });
    } else {
      LOGGER.info("No Records found for given session ID");
      return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
    }
    result.put(JsonConstants.CONTENT, taxonomyKpiArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

}
