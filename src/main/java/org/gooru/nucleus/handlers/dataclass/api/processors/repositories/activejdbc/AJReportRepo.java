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
    public MessageResponse getStudPerfDailyClassActivity() {
    	return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudPerfDailyClassActivityHandler(context));
    }
    
    @Override
    public MessageResponse getStudPerfMultipleCollections() {
    	return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudPerfMultipleCollectionHandler(context));
    }
    
    @Override
    public MessageResponse getStudPerfCourseCollections() {
    	return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudPerfCourseCollectionHandler(context));
    }
    
    @Override
    public MessageResponse getStudPerfCourseAssessments() {
    	return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildStudPerfCourseAssessmentHandler(context));
    }
    
    @Override
    public MessageResponse getLearnerPerformanceInCourse() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildLearnerCoursePerfHandler(context));
    } 
   
    @Override
    public MessageResponse getLearnerPerformanceInUnit() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildLearnerUnitPerfHandler(context));
    }
    
    @Override
    public MessageResponse getLearnerPerformanceInLesson() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildLearnerLessonPerfHandler(context));
    }
    
    @Override
    public MessageResponse getLearnerPerformanceInAssessment() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildLearnerAssessmentPerfHandler(context));
    }
    
    @Override
    public MessageResponse getLearnerPerformanceInIndependentAssessment() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildLearnerIndependentAssessmentPerfHandler(context));
    }
    
    @Override
    public MessageResponse getIndependentLearnerCourses() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndependentLearnerCourses(context));
    }
    
    @Override
    public MessageResponse getIndependentLearnerLocation() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerLocationHandler(context));
    }

    @Override
    public MessageResponse getIndependentLearnerPerformance() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerPerformanceHandler(context));
    }

    @Override
    public MessageResponse getIndLearnerCoursesLocation() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerCoursesLocationHandler(context));
    }
    
    @Override
    public MessageResponse getIndLearnerAssessmentsLocation() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerAssessmentsLocationHandler(context));
    }
    
    @Override
    public MessageResponse getIndLearnerCollectionsLocation() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerCollectionsLocationHandler(context));
    }

    @Override
    public MessageResponse getIndLearnerAllCoursesPerf() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerAllCoursesPerfHandler(context));
    }
    
    @Override
    public MessageResponse getIndLearnerIndAssessmentsPerf() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerIndAssessmentsPerfHandler(context));
    }
    
    @Override
    public MessageResponse getIndLearnerIndCollectionsPerf() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerIndCollectionsPerfHandler(context));
    }

    @Override
    public MessageResponse getIndLearnerCourseCollectionsPerf() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerCourseCollectionsPerfHandler(context));
    }
    
    @Override
    public MessageResponse getIndLearnerCourseAssessmentsPerf() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerCourseAssessmentsPerfHandler(context));
    }

    @Override
    public MessageResponse getIndLearnerTaxSubjects() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildIndLearnerTaxSubjectHandler(context));
    }
    
    @Override
    public MessageResponse getLearnerCourses() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildLearnerCourses(context));
    }
    
  
}
