package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.StudentRepo;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */
class AJStudentRepo implements StudentRepo {
    private final ProcessorContext context;

    public AJStudentRepo(ProcessorContext context) {
        this.context = context;
    }
  
    @Override
    public MessageResponse getStudentPeersInCourse() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentPeersInCourseHandler(context));
    }

    @Override
    public MessageResponse getStudentPeersInUnit() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentPeersInUnitHandler(context));
    }
    
    @Override
    public MessageResponse getStudentPeersInLesson() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentPeersInLessonHandler(context));
    }
    
    @Override
    public MessageResponse getStudentCurrentLocation() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentCurrentLocationHandler(context));
    }
    
    @Override
    public MessageResponse getStudentPerformanceInCourse() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentCoursePerfHandler(context));
    } 
   
    @Override
    public MessageResponse getStudentPerformanceInUnit() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentUnitPerfHandler(context));
    }
    
    @Override
    public MessageResponse getStudentPerformanceInLesson() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentLessonPerfHandler(context));
    }
    
    @Override
    public MessageResponse getStudentPerformanceInCollection() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentCollectionPerfHandler(context));
    }
    
    @Override
    public MessageResponse getStudentPerformanceInAssessment() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentAssessmentPerfHandler(context));
    }
    
    @Override
    public MessageResponse getSessionStatus() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildSessionStatusHandler(context));
    }

    @Override
    public MessageResponse getUserAssessmentSessions() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildUserAssessmentSessionsHandler(context));
    }
    
    @Override
    public MessageResponse getUserCollectionSessions() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildUserCollectionSessionsHandler(context));
    }
    
}
