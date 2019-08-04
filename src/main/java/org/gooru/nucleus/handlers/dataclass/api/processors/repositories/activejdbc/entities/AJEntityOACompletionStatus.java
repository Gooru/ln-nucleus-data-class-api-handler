package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** 
 * @author renuka
 */
@Table("offline_activity_completion_status")
public class AJEntityOACompletionStatus extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOACompletionStatus.class);
  
  public static final String ID = "id";
  public static final String OA_ID = "oa_id";
  public static final String OA_DCA_ID = "oa_dca_id";
  public static final String CLASS_ID = "class_id";
  public static final String STUDENT_ID = "student_id";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String COLLECTION_TYPE = "collection_type";
  public static final String IS_MARKED_BY_STUDENT = "is_marked_by_student";
  public static final String IS_MARKED_BY_TEACHER = "is_marked_by_teacher";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";  

  public static final String GET_CA_OA_MARKED_AS_COMPLETE_BY_STUDENT =
      "select student_id from offline_activity_completion_status where class_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id = ? AND content_source = ? and is_marked_by_student = true and is_marked_by_teacher = false";
  
  public static final String GET_CM_OA_MARKED_AS_COMPLETE_BY_STUDENT =
      "select student_id from offline_activity_completion_status where class_id = ?::uuid AND course_id = ?::uuid AND unit_id = ?::uuid AND lesson_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id IS NULL AND content_source = ? and is_marked_by_student = true and is_marked_by_teacher = false";
  
  public static final String GET_CM_OA_TO_TEACHER_GRADE =
      "SELECT oa_id as collection_id, collection_type, student_id as actor_id, course_id, unit_id, lesson_id, path_id, path_type "
          + "FROM offline_activity_completion_status WHERE class_id = ?::uuid AND course_id = ?::uuid "
          + "AND is_teacher_graded = false AND (is_marked_by_student = true OR is_marked_by_teacher = true) "
          + "AND collection_type = 'offline-activity' AND content_source = 'coursemap' "
          + "ORDER BY collection_id, actor_id, updated_at DESC";
  
  public static final String GET_CM_OA_TO_SELF_GRADE =
      "SELECT oa_id AS collection_id, collection_type, student_id AS actor_id, course_id, unit_id, lesson_id, path_id, path_type "
          + "FROM offline_activity_completion_status WHERE class_id = ?::uuid AND course_id = ?::uuid AND student_id = ?::uuid "
          + "AND is_teacher_graded = false AND is_student_graded = false AND has_student_rubric = true "
          + "AND (is_marked_by_student = true OR is_marked_by_teacher = true) "
          + "AND collection_type = 'offline-activity' AND content_source = 'coursemap' "
          + "ORDER BY collection_id, actor_id, updated_at DESC";

  public static final String GET_DISTINCT_STUDENTS_FOR_THIS_CM_OA =
      "SELECT DISTINCT (student_id) FROM offline_activity_completion_status WHERE class_id = ?::uuid "
          + "AND course_id = ?::uuid AND oa_id = ?::uuid AND content_source = 'coursemap' "
          + "AND is_teacher_graded = false AND (is_marked_by_student = true OR is_marked_by_teacher = true) "
          + "AND collection_type = 'offline-activity'";
  
  public static final String GET_CA_OA_TO_SELF_GRADE =
      "SELECT oa_id AS collection_id, collection_type, student_id AS actor_id, oa_dca_id, path_id, path_type "
          + "FROM offline_activity_completion_status WHERE class_id = ?::uuid AND oa_dca_id is not null AND student_id = ?::uuid "
          + "AND is_teacher_graded = false AND is_student_graded = false AND has_student_rubric = true "
          + "AND (is_marked_by_student = true OR is_marked_by_teacher = true) "
          + "AND collection_type = 'offline-activity' AND content_source = 'dailyclassactivity' "
          + "ORDER BY collection_id, actor_id, updated_at DESC";

  public static final String GET_DISTINCT_STUDENTS_FOR_THIS_CA_OA =
      "SELECT DISTINCT (student_id) FROM offline_activity_completion_status WHERE class_id = ?::uuid "
          + "AND oa_dca_id = ? AND content_source = 'dailyclassactivity' "
          + "AND is_teacher_graded = false AND (is_marked_by_student = true OR is_marked_by_teacher = true) "
          + "AND collection_type = 'offline-activity'";
  
  public static final String GET_CA_OA_TO_TEACHER_GRADE =
      "SELECT oa_id as collection_id, collection_type, student_id as actor_id, oa_dca_id, path_id, path_type "
          + "FROM offline_activity_completion_status WHERE class_id = ?::uuid AND oa_dca_id is not null "
          + "AND is_teacher_graded = false AND (is_marked_by_student = true OR is_marked_by_teacher = true) "
          + "AND collection_type = 'offline-activity' AND content_source = 'dailyclassactivity' "
          + "ORDER BY collection_id, actor_id, updated_at DESC";
  
}
