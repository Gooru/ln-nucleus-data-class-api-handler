package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */

public interface ReportRepo {
	
	MessageResponse getStudentPeersInCourse();
	
	MessageResponse getStudentPeersInUnit();
	
	MessageResponse getStudentPeersInLesson();
	
	MessageResponse getStudentCurrentLocation();
	
	MessageResponse getStudentPerformanceInCourse();
	
	MessageResponse getStudentPerformanceInUnit();
	
	MessageResponse getStudentPerformanceInLesson();

	MessageResponse getStudentPerformanceInAssessment();

	MessageResponse getStudentSummaryInCollection();
	
	MessageResponse getStudentSummaryInAssessment();
	
	MessageResponse getSessionStatus();
	
	MessageResponse getUserAssessmentSessions();
	
	MessageResponse getUserCollectionSessions();

	MessageResponse getSessionWiseTaxonmyReport();

    MessageResponse getAllStudentPerfInCourse();

    MessageResponse getAllStudentPerfInUnit();

    MessageResponse getStudentPerfInAllClasses();
    
    MessageResponse getStudentLocationInAllClasses();
    
    MessageResponse getStudPerfMultipleCollections();
    
    MessageResponse getStudPerfDailyClassActivity();
    
    MessageResponse getStudPerfCourseCollections();
    
    MessageResponse getStudPerfCourseAssessments();

}
