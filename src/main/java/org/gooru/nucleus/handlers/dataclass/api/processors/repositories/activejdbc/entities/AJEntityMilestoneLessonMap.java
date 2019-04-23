package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author mukul@gooru
 *
 */

@Table("milestone_lesson_map")
public class AJEntityMilestoneLessonMap extends Model {

	public static final String MILESTONE_ID =  "milestone_id";
	public static final String COURSE_ID = "course_id";
	public static final String UNIT_ID = "unit_id";
	public static final String LESSON_ID = "lesson_id";
	public static final String GRADE_ID = "grade_id";
	public static final String GRADE_NAME = "grade_name";
	public static final String GRADE_SEQ = "grade_seq";
	public static final String FW_CODE = "fw_code";
	public static final String TX_SUBJECT_CODE = "tx_subject_code";
	public static final String TX_DOMAIN_ID = "tx_domain_id";
	public static final String TX_DOMAIN_SEQ = "tx_domain_seq";
	public static final String TX_DOMAIN_CODE = "tx_domain_code";
	public static final String TX_COMP_CODE = "tx_comp_code";
	public static final String TX_COMP_NAME = "tx_comp_name";
	public static final String TX_COMP_STUDENT_DESC = "tx_comp_student_desc";
	public static final String TX_COMP_SEQ = "tx_comp_seq";
	public static final String UPDATED_AT = "updated_at";

	public static final String FETCH_MILESTONE_LESSON_IDS =
			"SELECT lesson_id, unit_id from milestone_lesson_map where milestone_id = ?";

	public static final String FETCH_MILESTONE_UNIT_IDS =
			"SELECT unit_id from milestone_lesson_map where milestone_id = ?";

	//************************************************************************************************************************

	//String Constants and Queries for STUDENT PERFORMANCE REPORTS IN UNIT    
	public static final String SELECT_DISTINCT_LESSON_ID_FOR_MILESTONE_ID =
			"SELECT DISTINCT(lesson_id) FROM milestone_lesson_map "
					+ "WHERE course_id = ? AND milestone_id = ? AND fw_code = ?";
	
	public static final String SELECT_DISTINCT_MILESTONE_ID_FOR_COURSE =
			"SELECT DISTINCT(milestone_id) FROM milestone_lesson_map "
					+ "WHERE course_id = ? AND fw_code = ?";

	//****************************************************************************************************************************************************************************************************


}
