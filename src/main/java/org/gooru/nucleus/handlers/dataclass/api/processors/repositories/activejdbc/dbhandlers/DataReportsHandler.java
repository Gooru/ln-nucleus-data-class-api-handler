package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DataReportsHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataReportsHandler.class);

  private final ProcessorContext context;

  int questionCount = 0;

  DataReportsHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
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
    JsonArray classArray = new JsonArray();
    JsonArray courseArray = new JsonArray();

    JsonObject classObject = new JsonObject();
    classObject.put(AJEntityBaseReports.ATTR_CLASS_ID, context.classId());
    classObject.put("classTitle", getTitle(context.classId()));
    classObject.put(AJEntityBaseReports.ATTR_USER_ID, context.getUserIdFromRequest());

    LazyList<AJEntityBaseReports> distinctCourses = AJEntityBaseReports.findBySQL(AJEntityBaseReports.NU_DISTINCT_COURSES,
            context.getUserIdFromRequest(), context.classId(), context.startDate(), context.endDate());

    distinctCourses.forEach(courses -> {
      JsonObject courseObject = new JsonObject();
      String courseId = courses.getString(AJEntityBaseReports.COURSE_GOORU_OID);
      courseObject.put(AJEntityBaseReports.ATTR_COURSE_ID, courseId);
      courseObject.put("courseTitle", getTitle(courseId));

      JsonArray unitArray = new JsonArray();

      LazyList<AJEntityBaseReports> distinctUnits = AJEntityBaseReports.findBySQL(AJEntityBaseReports.NU_DISTINCT_UNITS,
              context.getUserIdFromRequest(), context.classId(), courseId, context.startDate(), context.endDate());
      distinctUnits.forEach(units -> {
        JsonObject unitObject = new JsonObject();

        String unitId = units.getString(AJEntityBaseReports.UNIT_GOORU_OID);
        unitObject.put(AJEntityBaseReports.ATTR_UNIT_ID, unitId);
        unitObject.put("unitTitle", getTitle(unitId));

        JsonArray lessonArray = new JsonArray();

        LazyList<AJEntityBaseReports> distinctLessons = AJEntityBaseReports.findBySQL(AJEntityBaseReports.NU_DISTINCT_LESSONS,
                context.getUserIdFromRequest(), context.classId(), courseId, unitId, context.startDate(), context.endDate());

        distinctLessons.forEach(lesson -> {
          JsonObject lessonObject = new JsonObject();

          String lessonId = lesson.getString(AJEntityBaseReports.LESSON_GOORU_OID);
          lessonObject.put(AJEntityBaseReports.ATTR_LESSON_ID, lessonId);
          lessonObject.put("lessonTitle", getTitle(lessonId));
          JsonArray collectionArray = new JsonArray();

          LazyList<AJEntityBaseReports> distinctCollections = AJEntityBaseReports.findBySQL(AJEntityBaseReports.NU_DISTINCT_COLLECTIONS,
                  context.getUserIdFromRequest(), context.classId(), courseId, unitId, lessonId, context.startDate(), context.endDate());

          distinctCollections.forEach(collections -> {

            JsonObject collectionObject = new JsonObject();
            String collectionId = collections.getString(AJEntityBaseReports.COLLECTION_OID);
            collectionObject.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collectionId);
            collectionObject.put("collectionTitle", getTitle(collectionId));

            if (collections.getString(AJEntityBaseReports.COLLECTION_TYPE).equalsIgnoreCase(JsonConstants.COLLECTION)) {
              List<Map> collectionQuestionCount = Base.findAll(AJEntityBaseReports.NU_SELECT_COLLECTION_QUESTION_COUNT, context.classId(), courseId,
                      unitId, lessonId, collectionId, context.getUserIdFromRequest(), context.startDate(), context.endDate());

              // If questions are not present then Question Count is always
              // zero,
              // however this additional check needs to be added
              // since during migration of data from 3.0 chances are that QC
              // may
              // be
              // null instead of zero
              collectionQuestionCount.forEach(qc -> {
                if (qc.get(AJEntityBaseReports.QUESTION_COUNT) != null) {
                  questionCount = Integer.valueOf(qc.get(AJEntityBaseReports.QUESTION_COUNT).toString());
                }
              });

              List<Map> collectionData = Base.findAll(AJEntityBaseReports.NU_SELECT_COLLECTION_AGG_DATA, context.classId(), courseId, unitId,
                      lessonId, collectionId, context.getUserIdFromRequest(), context.startDate(), context.endDate());

              LOGGER.debug("Collection Attributes obtained");
              collectionData.forEach(m -> {
                ValueMapper.map(collectionObject, ResponseAttributeIdentifier.getSessionCollectionAttributesMap(), m);
                collectionObject.put(EventConstants.COLLECTION_TYPE, AJEntityBaseReports.ATTR_COLLECTION);
                collectionObject.put(JsonConstants.SCORE, Math.round(Double.valueOf(m.get(AJEntityBaseReports.SCORE).toString())));
                collectionObject.remove("gooruOId");
                double scoreInPercent = 0;
                int reaction = 0;
                if (questionCount > 0) {
                  Object collectionScore;
                  collectionScore = Base.firstCell(AJEntityBaseReports.NU_SELECT_COLLECTION_AGG_SCORE, context.classId(), courseId, unitId, lessonId,
                          collectionId, context.getUserIdFromRequest(), context.startDate(), context.endDate());

                  if (collectionScore != null) {
                    scoreInPercent = ((Double.valueOf(collectionScore.toString()) / questionCount) * 100);
                  }
                }
                LOGGER.debug("Collection score : {} - collectionId : {}", Math.round(scoreInPercent), collectionId);
                collectionObject.put(AJEntityBaseReports.SCORE, Math.round(scoreInPercent));
                Object collectionReaction;
                collectionReaction = Base.firstCell(AJEntityBaseReports.NU_SELECT_COLLECTION_AGG_REACTION, context.classId(), courseId, unitId,
                        lessonId, collectionId, context.getUserIdFromRequest(), context.startDate(), context.endDate());
                if (collectionReaction != null) {
                  reaction = Integer.valueOf(collectionReaction.toString());
                }
                LOGGER.debug("Collection reaction : {} - collectionId : {}", reaction, collectionId);
                collectionObject.put(AJEntityBaseReports.ATTR_REACTION, (reaction));
              });
              LOGGER.debug("Collection resource Attributes started");
              List<Map> assessmentQuestionsKPI = Base.findAll(AJEntityBaseReports.NU_SELECT_COLLECTION_RESOURCE_AGG_DATA, context.classId(), courseId,
                      unitId, lessonId, collectionId, context.getUserIdFromRequest(), context.startDate(), context.endDate());

              JsonArray questionsArray = new JsonArray();
              if (!assessmentQuestionsKPI.isEmpty()) {
                assessmentQuestionsKPI.forEach(questions -> {
                  JsonObject qnData = ValueMapper.map(ResponseAttributeIdentifier.getSessionCollectionResourceAttributesMap(), questions);
                  qnData.put(AJEntityBaseReports.ATTR_RESOURCE_ID, questions.get(AJEntityBaseReports.RESOURCE_ID));
                  qnData.remove("eventTime");
                  qnData.remove("gooruOId");
                  // TODO ; Data to synced ... Revisit in sometime..
                  qnData.put("metadata",getTitleAndTaxonomy(questions.get(AJEntityBaseReports.RESOURCE_ID).toString()));

                  if (questions.get(AJEntityBaseReports.ANSWER_OBECT) != null) {
                    qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(questions.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
                  }
                  // Default answerStatus will be skipped
                  if (qnData.getString(EventConstants.RESOURCE_TYPE).equalsIgnoreCase(EventConstants.QUESTION)) {
                    qnData.put(EventConstants.ANSWERSTATUS, EventConstants.SKIPPED);
                  }
                  qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(questions.get(AJEntityBaseReports.SCORE).toString())));
                  if (questionCount > 0) {
                    List<Map> questionScore = Base.findAll(AJEntityBaseReports.NU_SELECT_COLLECTION_QUESTION_AGG_SCORE, context.classId(), courseId,
                            unitId, lessonId, collectionId, questions.get(AJEntityBaseReports.RESOURCE_ID), context.getUserIdFromRequest(),
                            context.startDate(), context.endDate());

                    if (!questionScore.isEmpty()) {
                      questionScore.forEach(qs -> {
                        qnData.put(JsonConstants.SCORE, Math.round(Double.valueOf(qs.get(AJEntityBaseReports.SCORE).toString()) * 100));
                        qnData.put(JsonConstants.ANSWER_OBJECT, new JsonArray(qs.get(AJEntityBaseReports.ANSWER_OBECT).toString()));
                        qnData.put(EventConstants.ANSWERSTATUS, qs.get(AJEntityBaseReports.ATTR_ATTEMPT_STATUS).toString());
                        LOGGER.debug("Question Score : {} - resourceId : {}", qs.get(AJEntityBaseReports.SCORE).toString(),
                                questions.get(AJEntityBaseReports.RESOURCE_ID));
                      });
                    }
                  }
                  List<Map> resourceReaction = Base.findAll(AJEntityBaseReports.NU_SELECT_COLLECTION_RESOURCE_AGG_REACTION, context.classId(),
                          courseId, unitId, lessonId, collectionId, questions.get(AJEntityBaseReports.RESOURCE_ID), context.getUserIdFromRequest(),
                          context.startDate(), context.endDate());

                  if (!resourceReaction.isEmpty()) {
                    resourceReaction.forEach(rs -> {
                      qnData.put(JsonConstants.REACTION, Integer.valueOf(rs.get(AJEntityBaseReports.REACTION).toString()));
                      LOGGER.debug("Resource reaction: {} - resourceId : {}", rs.get(AJEntityBaseReports.REACTION).toString(),
                              questions.get(AJEntityBaseReports.RESOURCE_ID));
                    });
                  }
                  questionsArray.add(qnData);
                });
                collectionObject.put(JsonConstants.RESOURCES, questionsArray);
                LOGGER.debug("Collection Attributes obtained");
              }

            } else {
              // collectionType = assessment
              LazyList<AJEntityBaseReports> assesmentScoreReaction =
                      AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_NU_REPORT_ASSESSMENT_SCORE_REACTION, context.getUserIdFromRequest(),
                              context.classId(), courseId, unitId, lessonId, collectionId, context.startDate(), context.endDate());
              assesmentScoreReaction.forEach(scoreReaction -> {
                collectionObject.put(AJEntityBaseReports.ATTR_SCORE,
                        Math.round(Double.valueOf(scoreReaction.get(AJEntityBaseReports.SCORE).toString())));
                collectionObject.put(AJEntityBaseReports.ATTR_REACTION,
                        Math.round(Double.valueOf(scoreReaction.get(AJEntityBaseReports.REACTION).toString())));

              });
              collectionObject.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, AJEntityBaseReports.ATTR_ASSESSMENT);
              LazyList<AJEntityBaseReports> assesmentAttemptsTs =
                      AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_NU_REPORT_ASSESSMENT_TS_ATTEMPTS, context.getUserIdFromRequest(),
                              context.classId(), courseId, unitId, lessonId, collectionId, context.startDate(), context.endDate());

              assesmentAttemptsTs.forEach(attemptsTs -> {
                collectionObject.put(AJEntityBaseReports.ATTR_TIME_SPENT, attemptsTs.getLong(AJEntityBaseReports.TIME_SPENT));
                collectionObject.put(AJEntityBaseReports.ATTR_ATTEMPTS, attemptsTs.getLong(AJEntityBaseReports.VIEWS));

              });

              LazyList<AJEntityBaseReports> qnScoreReaction = AJEntityBaseReports.findBySQL(
                      AJEntityBaseReports.GET_NU_REPORT_ASSESSMENT_QUESTIONS_SCORE_REACTION, context.getUserIdFromRequest(), context.classId(),
                      courseId, unitId, lessonId, collectionId, context.startDate(), context.endDate());
              JsonArray questionsArray = new JsonArray();
              qnScoreReaction.forEach(scoreReaction -> {
                JsonObject qnData = new JsonObject();
                //TODO : TO BE REVISITED.
                qnData.put("metadata",getTitleAndTaxonomy(scoreReaction.get(AJEntityBaseReports.RESOURCE_ID).toString()));


                qnData.put(AJEntityBaseReports.ATTR_QUESTION_ID, scoreReaction.get(AJEntityBaseReports.RESOURCE_ID));
                qnData.put(JsonConstants.RESOURCE_TYPE, scoreReaction.get(AJEntityBaseReports.RESOURCE_TYPE));
                qnData.put(JsonConstants.QUESTION_TYPE, scoreReaction.get(AJEntityBaseReports.QUESTION_TYPE));
                qnData.put(JsonConstants.REACTION, scoreReaction.get(AJEntityBaseReports.REACTION));
                qnData.put(JsonConstants.ANSWER_OBJECT,
                        scoreReaction.get(AJEntityBaseReports.ANSWER_OBECT) != null
                                ? new JsonArray(scoreReaction.get(AJEntityBaseReports.ANSWER_OBECT).toString())
                                : null);
                // Rubrics - Score should be NULL only incase of OE questions
                qnData.put(JsonConstants.SCORE,
                        scoreReaction.get(AJEntityBaseReports.SCORE) != null
                                ? Math.round(Double.valueOf(scoreReaction.get(AJEntityBaseReports.SCORE).toString()))
                                : "NA");
                LazyList<AJEntityBaseReports> qnTSAttempts =
                        AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_NU_REPORT_ASSESSMENT_QUESTIONS_TS_ATTEMPTS,
                                context.getUserIdFromRequest(), context.classId(), courseId, unitId, lessonId, collectionId,
                                scoreReaction.get(AJEntityBaseReports.RESOURCE_ID).toString(), context.startDate(), context.endDate());
                qnTSAttempts.forEach(tsAttempts -> {
                  qnData.put(AJEntityBaseReports.ATTR_TIME_SPENT, tsAttempts.get(AJEntityBaseReports.TIME_SPENT));
                  qnData.put(AJEntityBaseReports.ATTR_ATTEMPTS, tsAttempts.get(AJEntityBaseReports.VIEWS));
                });
                questionsArray.add(qnData);
              });
              collectionObject.put("questions", questionsArray);
            }

            collectionArray.add(collectionObject);

          });
          lessonObject.put("performance", collectionArray);
          lessonArray.add(lessonObject);
        });
        unitObject.put("lessons", lessonArray);
        unitArray.add(unitObject);

      });
      courseObject.put("units", unitArray);
      courseArray.add(courseObject);
    });
    classObject.put("courses", courseArray);
    classArray.add(classObject);

    JsonObject result = new JsonObject();
    result.put("class", classArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

  private Object getTitle(String id) {
    return Base.firstCell(AJEntityContent.GET_TITLE, id);
  }

  private JsonObject getTitleAndTaxonomy(String id) {
    List<Map> meta = Base.findAll(AJEntityContent.GET_TTITLE_TAXONOMY, id);
    JsonObject metaData = new JsonObject();
    meta.forEach(me -> {
      metaData.put(AJEntityContent.TITLE, me.get(AJEntityContent.TITLE));
      metaData.put(AJEntityContent.TAXONOMY,
              me.get(AJEntityContent.TAXONOMY) != null ? new JsonObject(me.get(AJEntityContent.TAXONOMY).toString()) : null);
    });
    return metaData;
  }
}
