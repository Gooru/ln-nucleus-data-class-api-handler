package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

@Table("course_collection_count")
public class AJEntityCourseCollectionCount extends Model {

  public static final String COURSE_ID = "course_id";
  public static final String UNIT_ID = "unit_id";
  public static final String LESSON_ID = "lesson_id";
  public static final String COLLECTION_FORMAT = "collection_format";
  public static final String COLLECTION_COUNT = "collection_count";
  public static final String ASSESSMENT_COUNT = "assessment_count";
  public static final String EXT_ASSESSMENT_COUNT = "ext_assessment_count";
  public static final String EXT_COLLECTION_COUNT = "ext_collection_count";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";

  // collection format type :{collection,assessment,external-assessment}
  public static final String ATTR_ASSESSMENT = "assessment";
  public static final String ATTR_EXTERNAL_ASSESSMENT = "assessment-external";
  public static final String ATTR_COLLECTION = "collection";

  public static final String GET_COURSE_ASSESSMENT_COUNT =
      "SELECT (SUM(assessment_count) + SUM(ext_assessment_count)) as totalCount FROM course_collection_count WHERE course_id = ?";

  public static final String GET_UNIT_ASSESSMENT_COUNT =
      "SELECT (SUM(assessment_count) + SUM(ext_assessment_count)) as totalCount FROM course_collection_count WHERE "
          + "course_id = ? AND unit_id = ?";

  public static final String GET_LESSON_ASSESSMENT_COUNT =
      "SELECT (SUM(assessment_count) + SUM(ext_assessment_count)) as totalCount FROM course_collection_count WHERE "
          + "course_id = ? AND unit_id = ? and lesson_id = ?";

  public static final String GET_MILESTONE_LESSON_ASSESSMENT_COUNT =
      "SELECT (SUM(assessment_count) + SUM(ext_assessment_count)) as totalCount FROM course_collection_count WHERE "
          + "course_id = ? AND lesson_id = ?";

  public static final String GET_MILESTONE_ASSESSMENT_COUNT =
      "SELECT (SUM(assessment_count) + SUM(ext_assessment_count)) as totalCount FROM course_collection_count WHERE "
          + "course_id = ? AND lesson_id = ANY(?::varchar[])";

  public static final String GET_MILESTONE_LESSON_COLLECTION_COUNT =
      "SELECT (SUM(collection_count) + SUM(ext_collection_count)) as totalCount FROM course_collection_count WHERE "
          + "course_id = ? AND lesson_id = ?";

  public static final String GET_MILESTONE_COLLECTION_COUNT =
      "SELECT (SUM(collection_count) + SUM(ext_collection_count)) as totalCount FROM course_collection_count WHERE "
          + "course_id = ? AND lesson_id = ANY(?::varchar[])";

}
