package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * 
 * @author daniel
 *
 */
@Table("user_tax_subject")
public class AJEntityUserTaxonomySubject extends Model {

  public static final String ID = "id";

  public static final String TAX_SUBJECT_ID = "tax_subject_id";

  public static final String COURSE_ID = "course_id";

  public static final String ACTOR_ID = "actor_id";

  public static final String UPDATED_AT = "updated_at";

  public static final String SELECT_SUBJECT_ID_BY_COURSE =
      "SELECT tax_subject_id FROM content WHERE id = ?";

  public static final String ATTR_TAX_SUBJECT_ID = "taxSubjectId";

  public static final String ATTR_TAX_SUBJECT_TITLE = "taxSubjectTitle";

  // ******GET LIST OF SUBJECTS STUDIED BY THE
  // USER*******************************************************************************
  public static final String GET_IL_TAX_SUBJECTS =
      "SELECT DISTINCT tax_subject_id FROM user_tax_subject WHERE class_id IS NULL AND actor_id = ?";

  public static final String GET_LEARNER_TAX_SUBJECTS =
      "SELECT DISTINCT tax_subject_id FROM user_tax_subject WHERE actor_id = ?";

  public static final String GET_SUBJECT_TITLE = "SELECT  title FROM taxonomy_subject WHERE id = ?";

  public static final String GET_INDEPENDENT_LEARNER_COURSES =
      "SELECT DISTINCT course_id FROM user_tax_subject "
          + "WHERE actor_id = ? AND class_id IS NULL";

  public static final String GET_INDEPENDENT_LEARNER_ALL_COURSES =
      "SELECT DISTINCT course_id FROM user_tax_subject "
          + "WHERE actor_id = ? AND class_id IS NULL";

  public static final String GET_LEARNER_COURSES =
      "SELECT DISTINCT course_id,class_id FROM user_tax_subject "
          + "WHERE tax_subject_id = ? AND actor_id = ?";

  public static final String GET_LEARNER_ALL_COURSES =
      "SELECT DISTINCT course_id,class_id FROM user_tax_subject " + "WHERE actor_id = ?";

  public AJEntityUserTaxonomySubject() {
    // Turning off create_at and updated_at columns are getting updated by
    // activeJDBC.
    this.manageTime(false);
  }
}
