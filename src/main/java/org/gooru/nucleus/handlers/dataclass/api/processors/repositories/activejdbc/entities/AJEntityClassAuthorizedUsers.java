package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * 
 * @author insightsTeam
 *
 */
@Table("class_authorized_users")
public class AJEntityClassAuthorizedUsers extends Model{
  public static final String CLASS_ID = "class_id";
  public static final String CREATOR_ID = "creator_id";
  public static final String COLLABORATOR_ID = "collaborator_id";
  public static final String MODIFIED = "modified";
  public static final String SELECT_CLASS_CREATOR = "SELECT * FROM class_authorized_users WHERE class_id = ? AND creator_id = ?";
  public static final String SELECT_CLASS_COLLABORATOR = "SELECT * FROM class_authorized_users WHERE class_id = ? AND collaborator_id = ?";
  
}
