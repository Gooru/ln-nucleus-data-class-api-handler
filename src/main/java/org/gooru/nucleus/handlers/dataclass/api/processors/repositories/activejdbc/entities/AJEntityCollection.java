package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import java.util.List;
import org.gooru.nucleus.handlers.dataclass.api.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author renuka
 */
@DbName("coreDb")
@Table("collection")
public class AJEntityCollection extends Model {

  public static AJEntityCollection fetchCollection(String collectionId) throws Throwable {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.connection().setReadOnly(true);
      coreDb.openTransaction();
      List<AJEntityCollection> results = AJEntityCollection.findBySQL(
          "select title from collection where id = ?::uuid and is_deleted = ?", collectionId,
          false);
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

  public static AJEntityCollection fetchCollectionByLesson(String collectionId, String lessonId)
      throws Throwable {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.connection().setReadOnly(true);
      coreDb.openTransaction();
      List<AJEntityCollection> results = AJEntityCollection.findBySQL(
          "select title from collection where id = ?::uuid and lesson_id = ?::uuid and is_deleted = ?",
          collectionId, lessonId, false);
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
