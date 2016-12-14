package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */

public interface TeacherRepo {
		
	MessageResponse getAllStudentPerfInCourse();
	
	MessageResponse getAllStudentPerfInUnit();

}
