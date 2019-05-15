package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mukul@gooru
 *
 */

@Table("student_rubric_grading")
public class AJEntityRubricGrading extends Model {


  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityRubricGrading.class);

  public static final String ID = "id";
  public static final String EVENT_NAME = "event_name";
  public static final String RUBRIC_ID = "rubric_id";
  public static final String TITLE = "title";
  public static final String URL = "url";
  public static final String DESCRIPTION = "description";
  public static final String METADATA = "metadata";
  public static final String TAXONOMY = "taxonomy";
  public static final String GUT_CODES = "gut_codes";

  public static final String CREATOR_ID = "creator_id";
  public static final String MODIFIER_ID = "modifier_id";
  public static final String ORIGINAL_CREATOR_ID = "original_creator_id";
  public static final String ORIGINAL_RUBRIC_ID = "original_rubric_id";
  public static final String PARENT_RUBRIC_ID = "parent_rubric_id";
  public static final String PUBLISH_DATE = "publish_date";
  public static final String RUBRIC_CREATED_AT = "rubric_created_at";
  public static final String RUBRIC_UPDATED_AT = "rubric_updated_at";

  public static final String TENANT = "tenant";
  public static final String TENANT_ROOT = "tenant_root";


  public static final String STUDENT_ID = "student_id";
  public static final String CLASS_ID = "class_id";
  public static final String COURSE_ID = "course_id";
  public static final String UNIT_ID = "unit_id";
  public static final String LESSON_ID = "lesson_id";
  public static final String COLLECTION_ID = "collection_id";
  public static final String SESSION_ID = "session_id";
  public static final String RESOURCE_ID = "resource_id";

  public static final String MAX_SCORE = "max_score";
  public static final String STUDENT_SCORE = "student_score";
  public static final String CATEGORY_SCORE = "category_score";

  public static final String OVERALL_COMMENT = "overall_comment";
  public static final String GRADER = "grader";
  public static final String GRADER_ID = "grader_id";

  public static final String CREATE_TIMESTAMP = "created_at";
  public static final String UPDATE_TIMESTAMP = "updated_at";

  public static final String ATTR_CLASS_ID = "classId";
  public static final String ATTR_COURSE_ID = "courseId";
  public static final String ATTR_UNIT_ID = "unitId";
  public static final String ATTR_LESSON_ID = "lessonId";
  public static final String ATTR_RESOURCE_ID = "resourceId";
  public static final String ATTR_PATH_ID = "pathId";
  public static final String ATTR_COLLECTION_TYPE = "collectionType";
  public static final String ATTR_STUDENTS = "students";
  public static final String ATTR_LAST_ACCESSED = "lastaccessed";
  public static final String ATTR_ANSWER_TEXT = "answerText";
  public static final String ATTR_QUESTION_TEXT = "questionText";
  public static final String ATTR_QUESTION_ID = "questionId";
  public static final String ATTR_STUDENT_ID = "studentId";
  public static final String ATTR_STUDENT_SCORE = "studentScore";
  public static final String ATTR_MAX_SCORE = "maxScore";
  public static final String ATTR_CATEGORY_SCORE = "categoryScore";
  public static final String ATTR_OVERALL_COMMENT = "overallComment";


  public static final String GET_RUBRIC_GRADE_FOR_QUESTION =
      "SELECT student_id, student_score, max_score, "
          + "overall_comment, category_score from student_rubric_grading where class_id = ? AND "
          + "course_id = ? AND collection_id = ? AND resource_id = ? AND student_id = ? AND session_id = ?";

  public static final String GET_RUBRIC_GRADE_FOR_DCA_QUESTION =
      "SELECT student_id, student_score, max_score, "
          + "overall_comment, category_score from student_rubric_grading where class_id = ? AND "
          + "collection_id = ? AND resource_id = ? AND student_id = ? AND session_id = ? "
          + "AND date_in_time_zone = ?";

}
