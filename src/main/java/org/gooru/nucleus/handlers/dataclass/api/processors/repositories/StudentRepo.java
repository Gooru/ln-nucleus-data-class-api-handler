package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */

public interface StudentRepo {
	
	MessageResponse getStudentPeersInCourse();
	
	MessageResponse getStudentPeersInUnit();
	
	MessageResponse getStudentPeersInLesson();
	
	MessageResponse getStudentCurrentLocation();
	
	MessageResponse getStudentPerformanceInCourse();
	
	MessageResponse getStudentPerformanceInUnit();
	
	MessageResponse getStudentPerformanceInLesson();
	
	MessageResponse getStudentPerformanceInCollection();
	
	MessageResponse getStudentPerformanceInAssessment();
	
	MessageResponse getSessionStatus();
	
	MessageResponse getUserAssessmentSessions();
	
	MessageResponse getUserCollectionSessions();

}
