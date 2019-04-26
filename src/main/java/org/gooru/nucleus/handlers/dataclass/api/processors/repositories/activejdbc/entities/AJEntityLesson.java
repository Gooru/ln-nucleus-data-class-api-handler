package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author renuka
 */
@DbName("coreDb")
@Table("lesson")
public class AJEntityLesson extends Model {

  public static final String TITLE = "title";

  public static final String FETCH_LESSON =
      "select l.unit_id, u.title as unitTitle, l.lesson_id, l.title as lessonTitle from lesson l inner join unit u on l.unit_id = u.unit_id where l.lesson_id = ?::uuid and l.course_id = ?::uuid and u.is_deleted = false and l.is_deleted = false";

  @SuppressWarnings("rawtypes")
  public static Map<?, ?> fetchLesson(String courseId, String lessonId) throws Throwable {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.connection().setReadOnly(true);
      coreDb.openTransaction();
      List<Map> results = coreDb.findAll(FETCH_LESSON, lessonId, courseId);
      coreDb.commitTransaction();
      if (results != null && !results.isEmpty()) {
        return results.get(0);
      }
      return null;
    } catch (Throwable throwable) {
      coreDb.rollbackTransaction();
      throw throwable;
    } finally {
      coreDb.close();
    }
  }

}
