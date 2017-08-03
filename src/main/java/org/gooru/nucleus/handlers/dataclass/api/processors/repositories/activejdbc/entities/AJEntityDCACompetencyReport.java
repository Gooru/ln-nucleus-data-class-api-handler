package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Table("dca_competency_report")
public class AJEntityDCACompetencyReport extends Model {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityDCACompetencyReport.class);
	  public static final String ID = "id";
	  public static final String SESSION_ID = "session_id";
	  public static final String ACTOR_ID = "actor_id";   
	  public static final Object CLASS_ID = "class_id";
	  public static final String COURSE_ID = "course_id";
	  public static final String UNIT_ID = "unit_id";
	  public static final String LESSON_ID = "lesson_id";
	  public static final Object TAX_SUBJECT_ID = "tax_subject_id";
	  public static final String TAX_COURSE_ID = "tax_course_id";
	  public static final String TAX_DOMAIN_ID = "tax_domain_id";
	  public static final String TAX_STANDARD_ID = "tax_standard_id";
	  public static final String TAX_MICRO_STANDARD_ID = "tax_micro_standard_id";
	  public static final String TENANT_ID = "tenant_id";
	  public static final String DISPLAY_CODE = "display_code";
	  public static final String COLLECTION_ID = "collection_id";
	  public static final String RESOURCE_ID = "resource_id";
	  public static final String RESOURCE_TYPE = "resource_type";
	  public static final String EVENT_TYPE = "event_type";
	  public static final String COLLECTION_TYPE = "collection_type";
	  public static final String BASE_REPORT_ID = "base_report_id";
	  public static final String CREATED_AT = "created_at";
	  public static final String UPDATED_AT = "updated_at";  
	  
	  // Get Session wise taxonomy report from competency table..
	  public static final String SELECT_DCA_REPORT_IDS =
	          "SELECT tax_subject_id,tax_course_id,tax_domain_id,tax_standard_id,tax_micro_standard_id,display_code,string_agg(base_report_id||'', ',') AS base_report_id "
	                  + "FROM dca_competency_report WHERE session_id = ? " + "AND resource_type = 'question' "
	                  + "GROUP BY tax_subject_id,tax_course_id,tax_domain_id," + "tax_standard_id,tax_micro_standard_id,display_code";

	  public static final String GET_DCA_AGG_TAX_DATA = "SELECT sum(time_spent) AS time_spent, (AVG(score * 100)) AS score,"
	          + "ROUND(AVG(reaction)) AS reaction " + "FROM daily_class_activity WHERE id = ANY (?::integer[]) GROUP BY session_id";

	  public static final String GET_DCA_QUESTIONS_TAX_PERF =
	          "SELECT SUM(time_spent) AS time_spent, (SUM(score)*100) AS score ," + "SUM(reaction) AS reaction, SUM(views) AS views, question_type, "
	                  + "resource_id, MAX(resource_attempt_status) AS resource_attempt_status "
	                  + "FROM daily_class_activity WHERE id = ANY (?::integer[]) GROUP BY resource_id,question_type;";


}
