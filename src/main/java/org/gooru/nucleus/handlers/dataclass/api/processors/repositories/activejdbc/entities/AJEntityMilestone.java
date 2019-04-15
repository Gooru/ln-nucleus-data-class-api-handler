package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author mukul@gooru
 * 
 */

@Table("milestone")
public class AJEntityMilestone extends Model {

  public static final String ID = "id";
  public static final String MILESTONE_ID = "milestone_id";
  public static final String FW_CODE = "fw_code";
  public static final String COURSE_ID = "course_id";
  public static final String UNIT_ID = "unit_id";
  public static final String LESSON_ID = "lesson_id";

  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";

  public static final String SELECT_MILESTONE_ID_FROM_FRAMEWORK =
      "SELECT milestone_id FROM milestone "
          + "WHERE course_id = ? AND unit_id = ? AND lesson_id = ? AND fw_code = ?";


}
