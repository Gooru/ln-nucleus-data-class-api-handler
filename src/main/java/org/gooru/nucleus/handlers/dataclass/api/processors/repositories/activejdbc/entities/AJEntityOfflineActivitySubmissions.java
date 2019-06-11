package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * Created by renuka
 * 
 */
@Table("offline_activity_submissions")
public class AJEntityOfflineActivitySubmissions extends Model {

  public static final String ATTR_SUBMISSION_INFO = "submissionInfo";
  public static final String ATTR_SUBMISSION_TYPE = "submissionType";
  public static final String ATTR_SUBMISSION_SUBTYPE = "submissionSubtype";
  public static final String ATTR_SUBMISSION_TEXT = "submissionText";
  public static final String ATTR_CREATED_AT = "createdAt";
  public static final String ATTR_TASK_ID = "taskId";
  public static final String ATTR_SUBMISSIONS = "submissions";
  public static final String ATTR_TASKS = "tasks";
  public static final String ATTR_SUBMITTED_ON = "submittedOn";

  public static final String ID = "id";  
  public static final String OA_ID = "oa_id";
  public static final String OA_DCA_ID = "oa_dca_id";

  public static final String SUBMISSION_TYPE = "submission_type";
  public static final String SUBMISSION_SUBTYPE = "submission_subtype";
  public static final String CREATED_AT = "created_at";
  public static final String TASK_ID = "task_id";  
 
  public static final String SUBMISSION_INFO = "submission_info";
  public static final String SUBMISSION_TEXT = "submission_text";
  public static final String STUDENT_ID = "student_id";
  public static final String CLASS_ID = "class_id";
  public static final String UPDATED_AT = "updated_at";


  public static final String FETCH_OA_SUBMISSIONS =
      "class_id = ?::uuid and oa_dca_id  = ? and student_id = ?::uuid";
  
  public static final String FETCH_OA_LATEST_SUBMISSIONS =
      "class_id = ?::uuid and oa_dca_id  = ? and student_id = ?::uuid "
      + "and task_id = ? and submission_text IS NOT NULL order by updated_at desc";
  
}
