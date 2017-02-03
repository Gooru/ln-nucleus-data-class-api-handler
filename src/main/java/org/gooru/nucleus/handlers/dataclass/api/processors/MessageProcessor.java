package org.gooru.nucleus.handlers.dataclass.api.processors;

import java.util.ResourceBundle;
import java.util.UUID;

import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

class MessageProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
    private final Message<Object> message;
    private JsonObject request;
    
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
            LOGGER.error("Exception while getting Student performance in Course", t);
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
            LOGGER.error("Exception while getting Student performance in Course", t);
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
    
    //TEACHER REPORTS
    //********************************************************************************************************************************
   
    private MessageResponse getAllStudentPerfInCourse() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain All Students Performance Teacher. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("Course id not available to obtain All Student Performance for Teacher. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getAllStudentPerfInCourse();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting All Student performance in Course for Teacher", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }

    private MessageResponse getAllStudentPerfInUnit() {
    	try {
            ProcessorContext context = createContext();
            
            if (!checkClassId(context)) {
                LOGGER.error("ClassId not available to obtain Student Performance. Aborting!");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid ClassId");
            }
            
            if (!checkCourseId(context)) {
                LOGGER.error("Course id not available to obtain Student Performance. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid CourseId");
            }

            if (!checkUnitId(context)) {
                LOGGER.error("Unit id not available to get lesson. Aborting");
                return MessageResponseFactory.createInvalidRequestResponse("Invalid unitId");
            }
            
            return new RepoBuilder().buildReportRepo(context).getAllStudentPerfInUnit();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Student performance in Unit", t);
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
        LOGGER.debug("User ID from session :" + userId + " User ID From request : " + userUId);
        String sessionId = message.headers().get(MessageConstants.SESSION_ID);
        LOGGER.debug(sessionId);
        return new ProcessorContext(request, userId,userUId, classId, courseId, unitId, lessonId, collectionId, sessionId);
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

}
