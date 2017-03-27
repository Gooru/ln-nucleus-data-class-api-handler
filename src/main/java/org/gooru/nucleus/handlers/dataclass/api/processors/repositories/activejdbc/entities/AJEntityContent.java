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

  public static final String UPDATED_AT = "updated_at";

  public static final String SELECT_COURSE_TITLE = "SELECT title FROM content WHERE id = ?";

}
