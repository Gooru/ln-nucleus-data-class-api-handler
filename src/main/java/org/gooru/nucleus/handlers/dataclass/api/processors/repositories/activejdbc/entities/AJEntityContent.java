package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author daniel
 */
@Table("content")
public class AJEntityContent extends Model {

  public static final String ID = "id";

  public static final String CONTENT_FORMAT = "content_format";

  public static final String TITLE = "title";

  public static final String TAXONOMY = "taxonomy";

  public static final String UPDATED_AT = "updated_at";

  public static final String CLASS_CODE = "class_code";

  public static final String ATTR_CLASS_CODE = "classCode";
  
  public static final String ATTR_CLASS_TITLE = "classTitle";

  public static final String ATTR_COURSE_TITLE = "courseTitle";
  
  public static final String GET_TITLE = "SELECT title FROM content WHERE id = ?";

  public static final String GET_CLASS_TITLE_CODE = "SELECT title,class_code FROM content WHERE id = ?";

  public static final String GET_TTITLE_TAXONOMY = "SELECT title,taxonomy FROM content WHERE id = ?";

}
