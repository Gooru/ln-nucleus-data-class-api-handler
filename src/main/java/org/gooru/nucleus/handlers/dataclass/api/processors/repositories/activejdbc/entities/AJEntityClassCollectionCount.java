package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

@Table("class_collection_count")
public class AJEntityClassCollectionCount extends Model {

  public static final String CLASS_ID = "class_id";
  public static final String COURSE_ID = "course_id";
  public static final String UNIT_ID = "unit_id";
  public static final String LESSON_ID = "lesson_id";
  public static final String COLLECTION_FORMAT = "collection_format";
  public static final String COLLECTION_COUNT = "collection_count";
  public static final String ASSESSMENT_COUNT = "assessment_count";
  public static final String EXT_ASSESSMENT_COUNT = "ext_assessment_count";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";
  
  //collection format type :{collection,assessment,external-assessment}
  public static final String ATTR_ASSESSMENT = "assessment";
  public static final String ATTR_EXTERNAL_ASSESSMENT = "assessment-external";
  public static final String ATTR_COLLECTION = "collection";
  
  public static final String GET_CLASS_ASSESSMENT_COUNT = "SELECT SUM(assessment_count) as totalCount FROM class_collection_count WHERE class_id = ? GROUP BY class_id";

}
