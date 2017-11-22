package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author insightsTeam
 *
 */

@Table("competency_report")
public class AJEntityCompetencyReport extends Model {

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
  public static final String SELECT_BASE_REPORT_IDS =
          "SELECT tax_subject_id,tax_course_id,tax_domain_id,tax_standard_id,tax_micro_standard_id,display_code,string_agg(base_report_id||'', ',') AS base_report_id "
                  + "FROM competency_report WHERE session_id = ? " + "AND resource_type = 'question' "
                  + "GROUP BY tax_subject_id,tax_course_id,tax_domain_id," + "tax_standard_id,tax_micro_standard_id,display_code";

  public static final String GET_AGG_TAX_DATA = "SELECT sum(time_spent) AS time_spent, SUM(score) AS score, "
          + "ROUND(AVG(reaction)) AS reaction " + "FROM base_reports WHERE id = ANY (?::integer[]) GROUP BY session_id";

  public static final String GET_QUESTIONS_TAX_PERF =
          "SELECT SUM(time_spent) AS time_spent, (SUM(score)*100) AS score ," + "SUM(reaction) AS reaction, SUM(views) AS views, question_type, "
                  + "resource_id, MAX(resource_attempt_status) AS resource_attempt_status "
                  + "FROM base_reports WHERE id = ANY (?::integer[]) GROUP BY resource_id,question_type;";
  
  //Get max_score for Taxonomy Questions
  public static final String SELECT_TAXONOMY_MAX_SCORE = "SELECT SUM(max_score) AS max_score FROM "
          + "base_reports WHERE id = ANY (?::integer[]) GROUP BY session_id";

}
