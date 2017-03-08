package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.ReportRepo;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */
class AJReportRepo implements ReportRepo {
    private final ProcessorContext context;

    public AJReportRepo(ProcessorContext context) {
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
    public MessageResponse getStudentPerformanceInAssessment() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentAssessmentPerfHandler(context));
    }
    
    @Override
    public MessageResponse getStudentSummaryInCollection() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentCollectionSummaryHandler(context));
    }
    
    @Override
    public MessageResponse getStudentSummaryInAssessment() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentAssessmentSummaryHandler(context));
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

    @Override
    public MessageResponse getSessionWiseTaxonmyReport() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildSessionTaxonomyReportHandler(context));
    }
    
    @Override
    public MessageResponse getAllStudentPerfInCourse() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildAllStudentCoursePerfHandler(context));
    }

    @Override
    public MessageResponse getAllStudentPerfInUnit() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildAllStudentUnitPerfHandler(context));
    }
    
    @Override
    public MessageResponse getStudentPerfInAllClasses() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentPerfInAllClasses((context)));
    }
    
    @Override
    public MessageResponse getStudentLocationInAllClasses() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudentLocAllClassesHandler(context));
    }
    
    @Override
    public MessageResponse getStudPerfMultipleAssessments() {
    	return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudPerfMultipleAssessmentHandler(context));
    }
    
    @Override
    public MessageResponse getStudPerfMultipleCollections() {
    	return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudPerfMultipleCollectionHandler(context));
    }
}
