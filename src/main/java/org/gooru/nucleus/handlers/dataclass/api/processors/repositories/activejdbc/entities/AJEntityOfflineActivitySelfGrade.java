package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * Created by renuka
 * 
 */
@Table("offline_activity_self_grade")
public class AJEntityOfflineActivitySelfGrade extends Model {
  
  public static final String ATTR_STUDENT_SCORE = "studentScore";
  public static final String ATTR_MAX_SCORE = "maxScore";
  public static final String ATTR_TIME_SPENT = "timeSpent";
  public static final String ATTR_OVERALL_COMMENT = "overallComment";
  public static final String ATTR_CATEGORY_GRADE = "categoryGrade";
  public static final String ATTR_CONTENT_SOURCE = "contentSource";
  public static final String ATTR_GRADER = "grader";
  public static final String ATTR_RUBRIC_ID = "rubricId";
  public static final String ATTR_CREATED_AT = "createdAt";
  public static final String ATTR_OA_RUBRICS = "oaRubrics";
  public static final String ATTR_STUDENT_GRADES = "studentGrades";
  public static final String ATTR_TEACHER_GRADES = "teacherGrades";
  
  public static final String STUDENT_SCORE = "student_score";
  public static final String MAX_SCORE = "max_score";
  public static final String TIME_SPENT = "time_spent";
  public static final String OVERALL_COMMENT = "overall_comment";
  public static final String CATEGORY_GRADE = "category_grade";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String GRADER = "grader";
  public static final String RUBRIC_ID = "rubric_id";
  public static final String CREATED_AT = "created_at";

  public static final String FETCH_OA_SELF_GRADES =
      "class_id = ?::uuid and oa_id  = ?::uuid and student_id = ?::uuid";
}
