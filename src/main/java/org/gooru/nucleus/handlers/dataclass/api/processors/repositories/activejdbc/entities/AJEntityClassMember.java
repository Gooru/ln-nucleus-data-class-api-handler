package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
/**
 * 
 * @author 100041
 *
 */
@Table("class_member")
public class AJEntityClassMember extends Model {
  public static final String CLASS_ID = "class_id";
  public static final String USER_ID = "user_id";
  public static final String CREATED = "created";
  public static final String MODIFIED = "modified";
  public static final String CLASS_MEMBER_STATUS = "class_member_status";
  public static final String SELECT_CLASS_MEMBER = "SELECT * FROM class_member WHERE class_id = ? AND user_id = ?";


}
