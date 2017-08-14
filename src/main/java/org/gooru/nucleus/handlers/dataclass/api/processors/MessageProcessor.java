package org.gooru.nucleus.handlers.dataclass.api.processors;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.UUID;

import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

class MessageProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
    private final Message<Object> message;
    private JsonObject request;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public MessageProcessor(Message<Object> message) {
        this.message = message;
    }

    @Override
    public MessageResponse process() {
        MessageResponse result;
        try {
            // Validate the message itself
            ExecutionResult<MessageResponse> validateResult = validateAndInitialize();
            if (validateResult.isCompleted()) {
                return validateResult.result();
            }            
            final String msgOp = message.headers().get(MessageConstants.MSG_HEADER_OP);           
            
            //There should be only one handler
            switch (msgOp) {
            case MessageConstants.MSG_OP_COURSE_PEERS:
                result = getStudentPeersInCourse();                
                break;
            case MessageConstants.MSG_OP_UNIT_PEERS:
                result = getStudentPeersInUnit();                
            	break;
            case MessageConstants.MSG_OP_LESSON_PEERS:
            	result = getStudentPeersInLesson();
                break;
            case MessageConstants.MSG_OP_STUDENT_CURRENT_LOC:
            	result = getStudentCurrentLocation();
                break;
            case MessageConstants.MSG_OP_STUDENT_COURSE_PERF:            	
                result = getStudentPerfInCourse();
                break;
            case MessageConstants.MSG_OP_STUDENT_UNIT_PERF:
            	result = getStudentPerfInUnit();
                break;
            case MessageConstants.MSG_OP_STUDENT_LESSON_PERF:
            	result = getStudentPerfInLesson();
                break;
            case MessageConstants.MSG_OP_STUDENT_ASSESSMENT_PERF:
              result = getStudentPerfInAssessment();
                break;
            case MessageConstants.MSG_OP_STUDENT_COLLECTION_SUMMARY:
            	result = getStudentSummaryInCollection();
                break;
            case MessageConstants.MSG_OP_STUDENT_ASSESSMENT_SUMMARY:
            	result = getStudentSummaryInAssessment();            	
                break;
            case MessageConstants.MSG_OP_SESSION_STATUS:
            	result = getSessionStatus();            	
                break;
            case MessageConstants.MSG_OP_USER_ALL_ASSESSMENT_SESSIONS:
            	result = getUserAssessmentSessions();            	
                break;
            case MessageConstants.MSG_OP_USER_ALL_COLLECTION_SESSIONS:
            	result = getUserCollectionSessions();            	
                break;
            case MessageConstants.MSG_OP_SESSION_TAXONOMY_REPORT:
              result = getSessionWiseTaxonomyReport();              
                break;
            case MessageConstants.MSG_OP_ALL_STUDENT_CLASSES_PERF:
              result = getStudentPerfInAllClasses();    
              break;
            case MessageConstants.MSG_OP_STUDENT_LOC_ALL_CLASSES:
                result = getStudentLocInAllClasses();    
                break;
            case MessageConstants.MSG_OP_STUDENT_PERF_MULT_COLLECTION:
                result = getStudentPerfMultipleCollections();    
                break;
            case MessageConstants.MSG_OP_STUDENT_PERF_COURSE_ASSESSMENT:
                result = getStudentPerfCourseAssessments();    
                break;
            case MessageConstants.MSG_OP_STUDENT_PERF_COURSE_COLLECTION:
                result = getStudentPerfCourseCollections();    
                break;
            case MessageConstants.MSG_OP_INDEPENDENT_LEARNER_COURSE_PERF:
                result = getUserIndepedentLearningPerfInCourse();   
                break;
            case MessageConstants.MSG_OP_INDEPENDENT_LEARNER_UNIT_PERF:
                result = getUserIndepedentLearningPerfInUnit();
                break;
            case MessageConstants.MSG_OP_INDEPENDENT_LEARNER_LESSON_PERF:
                result = getUserIndepedentLearningPerfInLesson();
                break;
            case MessageConstants.MSG_OP_INDEPENDENT_LEARNER_ASSESSMENT_PERF:
                result = getUserIndepedentLearningPerfInAssessment();  
                break;
            case MessageConstants.MSG_OP_INDEPENDENT_LEARNER_INDEPENDENT_ASSESSMENT_PERF:
                result = getUserLearningPerfInIndepedentAssessment();
                break;
            case MessageConstants.MSG_OP_INDEPENDENT_LEARNER_COURSES:
                //result = getIndependentLearnerCourses();
                result = getLearnerCourses();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_ALL_LOCATION:
                result = getIndependentLearnerLoc();
                break;                    
            case MessageConstants.MSG_OP_IND_LEARNER_ALL_PERFORMANCE:
                result = getIndependentLearnerPerf();
                break;                
            case MessageConstants.MSG_OP_IND_LEARNER_CURRENT_LOC:
                result = getIndependentLearnerCoursesLoc();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_LOCATION_ALL_IND_ASSESSMENTS:
                result = getIndependentLearnerAssessmentsLoc();                
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_LOCATION_ALL_IND_COLLECTIONS:
                result = getIndependentLearnerCollectionsLoc();
                break;                
            case MessageConstants.MSG_OP_IND_LEARNER_ALL_COURSES_PERF:                
            	result = getIndependentLearnerAllCoursesPerf();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_PERF_ALL_IND_ASSESSMENTS:                
                result = getIndependentLearnerIndAssessmentsPerf();                
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_PERF_ALL_IND_COLLECTIONS:                
                result = getIndependentLearnerIndCollectionsPerf();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_COURSE_ALL_COLLECTIONS_PERF:
                result = getIndependentLearnerCourseCollectionsPerf();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_COURSE_ALL_ASSESSMENTS_PERF:
                result = getIndependentLearnerCourseAssessmentsPerf();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_TAX_SUBJECTS:
                result = getIndependentLearnerTaxSubjects();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_COLLECTION_SUMMARY:
            	result = getIndependentLearnerSummaryInCollection();
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_ASSESSMENT_SUMMARY:
            	result = getIndependentLearnerSummaryInAssessment();            	
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_ALL_ASSESSMENT_SESSIONS:
            	result = getIndependentLearnerAssessmentSessions();            	
                break;
            case MessageConstants.MSG_OP_IND_LEARNER_ALL_COLLECTION_SESSIONS:
            	result = getIndependentLearnerCollectionSessions();            	
                break;
                //Rubric Grading
            case MessageConstants.MSG_OP_RUBRICS_QUESTIONS_TO_GRADE:
                result = getRubricQuestionsToGrade();
                break;
            case MessageConstants.MSG_OP_RUBRIC_QUESTIONS_STUDENTS_LIST:
                result = getStudentsForRubricQue();
                break;
            case MessageConstants.MSG_OP_RUBRIC_QUESTIONS_STUDENT_ANSWERS:
                result = getStudAnswersForRubricQue();
                break;
            case MessageConstants.MSG_OP_RUBRIC_QUESTIONS_GRADE_SUMMARY:
                result = getRubricSummaryforQue();
                break;
                //DCA
            case MessageConstants.MSG_OP_STUDENT_PERF_DAILY_CLASS_ACTIVITY:
                result = getStudentPerfDailyClassActivity();    
                break;
            case MessageConstants.MSG_OP_DCA_STUDENT_COLLECTION_SUMMARY:
            	result = getStudentSummaryInDCACollection();
                break;
            case MessageConstants.MSG_OP_DCA_STUDENT_ASSESSMENT_SUMMARY:
            	result = getStudentSummaryInDCAAssessment();            	
                break;
            case MessageConstants.MSG_OP_DCA_SESSION_TAXONOMY_REPORT:
                result = getDCASessionTaxonomyReport();              
                  break;
            case MessageConstants.MSG_OP_NU_DATA_REPORT:
                result = getDataReports();
                break;
            default:
                LOGGER.error("Invalid operation type passed in, not able to handle");
                return MessageResponseFactory
                    .createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.operation"));
            }
            return result;
            
        } catch (Throwable e) {
            LOGGER.error("Unhandled exception in processing", e);
            return MessageResponseFactory.createInternalErrorResponse();
        }
    }
    
    //************ DAILY CLASS ACTIVITY ******************************************************************************************
    
    private MessageResponse getStudentSummaryInDCACollection() {
    	try {
            ProcessorContext context = createContext();
                        
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
             
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid userId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentSummaryInDCACollection();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student DCA Collection Summary Report", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }

    
    private MessageResponse getStudentSummaryInDCAAssessment() {
    	try {
            ProcessorContext context = createContext();
                        
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
             
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid userId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentSummaryInDCAAssessment();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student DCA Assessment Summary Report", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getDCASessionTaxonomyReport() {
        try {
              ProcessorContext context = createContext();
          
              if (!checkSessionId(context)) {
                  LOGGER.error("Session id not available in the request. Aborting");
                  return MessageResponseFactory.createInvalidRequestResponse("Invalid sessionId");
              }
              
              return new RepoBuilder().buildReportRepo(context).getDCASessionTaxonomyReport();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Student DCA Session Taxonomy Report", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }

      private MessageResponse getDataReports() {
        try {
          ProcessorContext context = createContext();
    
          LOGGER.info("classId : {}", context.classId());
          LOGGER.info("userId : {}", context.getUserIdFromRequest());
    
          LOGGER.info("startDate : {}", context.startDate());
          LOGGER.info("endDate : {}", context.endDate());
    
          if (!checkClassId(context)) {
            LOGGER.error("ClassId not available to obtain Student Current Location. Aborting!");
            return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
          }
    
          if (!validateUser(context.userIdFromSession())) {
            LOGGER.error("Invalid User ID. Aborting");
            return MessageResponseFactory.createInvalidRequestResponse("Invalid UserId");
          }
          if (!validateDate(context.startDate())) {
            LOGGER.error("Invalid start date. Aborting");
            return MessageResponseFactory.createInvalidRequestResponse("Invalid startDate");
          }
          if (!validateDate(context.endDate())) {
            LOGGER.error("Invalid end date. Aborting");
            return MessageResponseFactory.createInvalidRequestResponse("Invalid endDate");
          }
          return new RepoBuilder().buildReportRepo(context).getDataReports();
    
        } catch (Throwable t) {
          LOGGER.error("Exception while getting Student DCA Session Taxonomy Report", t);
          return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    
      }

    //************ RUBRIC GRADING ******************************************************************************************
    
    private MessageResponse getRubricQuestionsToGrade() {
    	try {
            ProcessorContext context = createContext();
            
            return new RepoBuilder().buildReportRepo(context).getRubricQuesToGrade();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Questions pending Grading", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }

    private MessageResponse getStudentsForRubricQue() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkQuestionId(context)) {
                LOGGER.error("QuestionId not available to obtain Student Ids. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid QuestionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentsForRubricQuestion();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student List for Rubric Grading", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getStudAnswersForRubricQue() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkQuestionId(context)) {
                LOGGER.error("QuestionId not available to obtain answers. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid QuestionId");
            }

            if (!checkStudentId(context)) {
                LOGGER.error("StudentId not available to obtain answers. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid StudentId");
            }

            
            return new RepoBuilder().buildReportRepo(context).getStudentAnswersForRubricQuestion();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student answers for Rubric Grading", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getRubricSummaryforQue() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Rubric Question Summary. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("Course id not available to obtain Student Rubric Question Summary. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }
            
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Student Rubric Question Summary. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }

            
            if (!checkQuestionId(context)) {
                LOGGER.error("QuestionId not available to obtain Rubric Question Summary. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid QuestionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getRubricSummaryforQuestion();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student answers for Rubric Grading", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    //**************************************************************************************************************
    
    private MessageResponse getStudentPeersInCourse() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Peers. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("Course Id not available to obtain Student Peers. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentPeersInCourse();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student peers in Course", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getStudentPeersInUnit() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Peers. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("Course id not available to obtain Student Peers. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("Unit id not available to obtain Student Peers. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentPeersInUnit();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student peers in Unit", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    
    private MessageResponse getStudentPeersInLesson() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Peers. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Student Peers. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Student Peers. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }

            if (!checkLessonId(context)) {
                LOGGER.error("LessonId not available to obtain Student Peers. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid LessonId");
            } 
            
            return new RepoBuilder().buildReportRepo(context).getStudentPeersInLesson();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student peers in Lesson", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    


   
    private MessageResponse getStudentCurrentLocation() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Current Location. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UserId");
            }

        return new RepoBuilder().buildReportRepo(context).getStudentCurrentLocation();
        
    } catch (Throwable t) {
        LOGGER.error("Exception while getting Student Current Location", t);
        return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
    }

}


    
    private MessageResponse getStudentPerfInCourse() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Performance. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentPerformanceInCourse();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in Course", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
   
    private MessageResponse getStudentPerfInUnit() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Performance. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentPerformanceInUnit();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in Unit", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getStudentPerfInLesson() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Performance. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }

            if (!checkLessonId(context)) {
                LOGGER.error("LessonId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid LessonId");
            } 
            
            return new RepoBuilder().buildReportRepo(context).getStudentPerformanceInLesson();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in Lesson", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getStudentPerfInAssessment() {
      try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Performance. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }

            if (!checkLessonId(context)) {
                LOGGER.error("LessonId not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid LessonId");
            } 
            if (!checkCollectionId(context)) {
              LOGGER.error("AssessmentId not available to obtain Student Performance. Aborting");
              return MessageResponseFactory.createInvalidRequestResponse("Invalid assessmentId");
            } 
            return new RepoBuilder().buildReportRepo(context).getStudentPerformanceInAssessment();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in Lesson", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getStudentSummaryInCollection() {
    	try {
            ProcessorContext context = createContext();
                        
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
             
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid userId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentSummaryInCollection();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student Collection Summary", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }

    
    private MessageResponse getStudentSummaryInAssessment() {
    	try {
            ProcessorContext context = createContext();
                        
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
             
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid userId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getStudentSummaryInAssessment();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student Assessment Summary", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getSessionStatus() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkSessionId(context)) {
                LOGGER.error("SessionId not available. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid SessionId");
            }
            
            if (!checkCollectionId(context)) {
                LOGGER.error("CollectionId not available to get Session Status. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CollectionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getSessionStatus();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student peers in Unit", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getUserAssessmentSessions() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain User Sessions. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getUserAssessmentSessions();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting User Sessions for Assessment", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getUserCollectionSessions() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain User Sessions. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getUserCollectionSessions();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting User Sessions for Collection", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
 
    private MessageResponse getSessionWiseTaxonomyReport() {
      try {
            ProcessorContext context = createContext();
        
            if (!checkSessionId(context)) {
                LOGGER.error("Session id not available in the request. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid sessionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getSessionWiseTaxonmyReport();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in Unit", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }

    //=================================User Independent Learning ===============================================//

    
    private MessageResponse getUserIndepedentLearningPerfInCourse() {
      try {
            ProcessorContext context = createContext();
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getLearnerPerformanceInCourse();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting learner performance in Course", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
   
    private MessageResponse getUserIndepedentLearningPerfInUnit() {
      try {
            ProcessorContext context = createContext();

            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getLearnerPerformanceInUnit();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting learner performance in Unit", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getUserIndepedentLearningPerfInLesson() {
      try {
            ProcessorContext context = createContext();

            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }

            if (!checkLessonId(context)) {
                LOGGER.error("LessonId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid LessonId");
            } 
            
            return new RepoBuilder().buildReportRepo(context).getLearnerPerformanceInLesson();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting learner performance in Lesson", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getUserIndepedentLearningPerfInAssessment() {
      try {
            ProcessorContext context = createContext();
          
            if (!checkCourseId(context)) {
                LOGGER.error("CourseId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("UnitId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid UnitId");
            }

            if (!checkLessonId(context)) {
                LOGGER.error("LessonId not available to obtain Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid LessonId");
            } 
            if (!checkCollectionId(context)) {
              LOGGER.error("AssessmentId not available to obtain Learner Performance. Aborting");
              return MessageResponseFactory.createInvalidRequestResponse("Invalid assessmentId");
            } 
            return new RepoBuilder().buildReportRepo(context).getLearnerPerformanceInAssessment();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting learner performance in assessment", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    private MessageResponse getUserLearningPerfInIndepedentAssessment() {
      try {
            ProcessorContext context = createContext();
            if (!checkCollectionId(context)) {
              LOGGER.error("AssessmentId not available to obtain learner assessment performance. Aborting");
              return MessageResponseFactory.createInvalidRequestResponse("Invalid assessmentId");
            } 
            return new RepoBuilder().buildReportRepo(context).getLearnerPerformanceInIndependentAssessment();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Learner performance in Lesson", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getLearnerCourses() {
      try {
            ProcessorContext context = createContext();
            return new RepoBuilder().buildReportRepo(context).getLearnerCourses();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting learner courses", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getIndependentLearnerLoc() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndependentLearnerLocation();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Independent Learner Location", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getIndependentLearnerPerf() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndependentLearnerPerformance();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Independent Learner Performance", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getIndependentLearnerCoursesLoc() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerCoursesLocation();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }

    private MessageResponse getIndependentLearnerAssessmentsLoc() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerAssessmentsLocation();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getIndependentLearnerCollectionsLoc() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerCollectionsLocation();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
     
    private MessageResponse getIndependentLearnerAllCoursesPerf() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerAllCoursesPerf();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }

    private MessageResponse getIndependentLearnerIndAssessmentsPerf() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerIndAssessmentsPerf();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getIndependentLearnerIndCollectionsPerf() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerIndCollectionsPerf();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    //----------------------
    
    private MessageResponse getIndependentLearnerCourseCollectionsPerf() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerCourseCollectionsPerf();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getIndependentLearnerCourseAssessmentsPerf() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getIndLearnerCourseAssessmentsPerf();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting independent learner courses", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }

    private MessageResponse getIndependentLearnerTaxSubjects() {
      try {
            ProcessorContext context = createContext();
            return new RepoBuilder().buildReportRepo(context).getIndLearnerTaxSubjects();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting independent learner tax subjects", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
     //Mukul 
    //*********************************************************************************************************************
    
    private MessageResponse getIndependentLearnerSummaryInCollection() {
    	try {
            ProcessorContext context = createContext();
                        
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Independent Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
             
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid userId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getIndLearnerSummaryInCollection();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Independent Learner Collection Summary", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }

    
    private MessageResponse getIndependentLearnerSummaryInAssessment() {
    	try {
            ProcessorContext context = createContext();
                        
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Independent Learner Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
             
            if (!validateUser(context.userIdFromSession())) {
                LOGGER.error("Invalid User ID. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid userId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getIndLearnerSummaryInAssessment();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Independent Learner Assessment Summary", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    
    private MessageResponse getIndependentLearnerAssessmentSessions() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Independent Learner Sessions. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getIndLearnerAssessmentSessions();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Independent Learner Sessions for Assessment", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    private MessageResponse getIndependentLearnerCollectionSessions() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkCollectionId(context)) {
                LOGGER.error("Collection id not available to obtain Independent Learner Sessions. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid collectionId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getIndLearnerCollectionSessions();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Independent Learner Sessions for Collection", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    //*********************************************************************************************************************
    
    private MessageResponse getStudentPerfInAllClasses() {
      try {
            ProcessorContext context = createContext();
            return new RepoBuilder().buildReportRepo(context).getStudentPerfInAllClasses();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in all Classes", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }
    
    
    private MessageResponse getStudentLocInAllClasses() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getStudentLocationInAllClasses();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Student Location in all Classes", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }

    private MessageResponse getStudentPerfMultipleCollections() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getStudPerfMultipleCollections();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Student Performance in Multiple Collections", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getStudentPerfDailyClassActivity() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getStudPerfDailyClassActivity();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Student Performance in Daily Class Activity.", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getStudentPerfCourseAssessments() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getStudPerfCourseAssessments();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Student Performance in Course Assessments", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }
    
    private MessageResponse getStudentPerfCourseCollections() {
        try {
              ProcessorContext context = createContext();
              return new RepoBuilder().buildReportRepo(context).getStudPerfCourseCollections();
              
          } catch (Throwable t) {
              LOGGER.error("Exception while getting Student Performance in Course Collections", t);
              return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
          }

      }

    
    private ProcessorContext createContext() {
    	String classId = message.headers().get(MessageConstants.CLASS_ID);
        String courseId = message.headers().get(MessageConstants.COURSE_ID);
        String unitId = message.headers().get(MessageConstants.UNIT_ID);
        String lessonId = message.headers().get(MessageConstants.LESSON_ID);
        String collectionId = message.headers().get(MessageConstants.COLLECTION_ID);
        /* user id from session */
        String userId =  (request).getString(MessageConstants._USER_ID);
        /* user id from api request */
        String userUId = (request).getString(MessageConstants.USER_UID);
        userUId = userUId == null ? (request).getString(MessageConstants.USER_ID) : userUId;        
        String sessionId = message.headers().get(MessageConstants.SESSION_ID);
        String studentId = message.headers().get(MessageConstants.STUDENT_ID);
        String questionId = message.headers().get(MessageConstants.QUESTION_ID);
        String startDate = message.headers().get(MessageConstants.START_DATE);
        String endDate = message.headers().get(MessageConstants.END_DATE);
        
        return new ProcessorContext(request, userId,userUId, classId, courseId, unitId, lessonId, collectionId, 
        		sessionId, studentId, questionId,startDate,endDate);
    }

    //This is just the first level validation. Each Individual Handler would need to do more validation based on the
    //handler specific params that are needed for processing.
    private ExecutionResult<MessageResponse> validateAndInitialize() {    	
        if (message == null || !(message.body() instanceof JsonObject)) {
            LOGGER.error("Invalid message received, either null or body of message is not JsonObject ");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.message")),
                ExecutionResult.ExecutionStatus.FAILED);
        }

        /** userId = ((JsonObject) message.body()).getString(MessageConstants.MSG_USER_ID);
        if (!validateUser(userId)) {
            LOGGER.error("Invalid user id passed. Not authorized.");
            return new ExecutionResult<>(
                MessageResponseFactory.createForbiddenResponse(RESOURCE_BUNDLE.getString("missing.user")),
                ExecutionResult.ExecutionStatus.FAILED);
        } **/

        request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);
        LOGGER.info(request.toString());
        
        if (request == null) {
            LOGGER.error("Invalid JSON payload on Message Bus");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")),
                ExecutionResult.ExecutionStatus.FAILED);
        }

        // All is well, continue processing
        return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);
    }
    
    private boolean checkCollectionId(ProcessorContext context) {
        return validateId(context.collectionId());
    }
    
    private boolean checkLessonId(ProcessorContext context) {
        return validateId(context.lessonId());
    }

    private boolean checkUnitId(ProcessorContext context) {
        return validateId(context.unitId());
    }

    private boolean checkCourseId(ProcessorContext context) {
        return validateId(context.courseId());
    }
    
    private boolean checkClassId(ProcessorContext context) {
        return validateId(context.classId());
    }
    
    private boolean checkSessionId(ProcessorContext context) {
        return validateId(context.sessionId());
    }
    
    private boolean checkQuestionId(ProcessorContext context) {
        return validateId(context.questionId());
    }
    
    private boolean checkStudentId(ProcessorContext context) {
        return validateId(context.studentId());
    }
    

 
    private boolean validateUser(String userId) {
        return !(userId == null || userId.isEmpty()
            || (userId.equalsIgnoreCase(MessageConstants.MSG_USER_ANONYMOUS)) && validateUuid(userId));
    }

    private boolean validateId(String id) {
        return !(id == null || id.isEmpty()) && validateUuid(id);
    }

    private boolean validateUuid(String uuidString) {
        try {
            UUID uuid = UUID.fromString(uuidString);
            return true;
        } catch (IllegalArgumentException e) {        	
            return false;
        } catch (Exception e) {        	
            return false;
        }
    }

    private boolean validateDate(String value) {
      Date date = null;
      if (value != null) {
        try {
          date = sdf.parse(value);
          if (!value.equals(sdf.format(date))) {
            date = null;
          }
        } catch (Exception ex) {
          LOGGER.error("Invalid date format...");
        }
      }
      return date != null;
    }
}
