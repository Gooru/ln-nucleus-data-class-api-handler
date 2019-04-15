package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.gooru.nucleus.handlers.dataclass.api.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.LazyList;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author renuka
 */

@DbName("coreDb")
@Table("user_navigation_paths")
public class AJEntityUserNavigationPaths extends Model {

  private static final String FETCH_SYSTEM_SUGGESTION =
      "suggested_content_id = ?::uuid and ctx_class_id = ?::uuid and ctx_course_id = ?::uuid and ctx_lesson_id = ?::uuid";

  public static AJEntityUserNavigationPaths fetchSystemSuggestedContent(String collId,
      String classId, String courseId, String lessonId) throws Throwable {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.connection().setReadOnly(true);
      coreDb.openTransaction();
      LazyList<AJEntityUserNavigationPaths> results = AJEntityUserNavigationPaths
          .find(FETCH_SYSTEM_SUGGESTION, collId, classId, courseId, lessonId);
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
