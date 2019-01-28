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
@Table("content")
public class AJEntityCoreContent extends Model {

  public static final String ID = "id";

  public static final String TITLE = "title";

  public static final String FETCH_CONTENT = "id = ?::uuid and collection_id = ?::uuid and is_deleted = ?";

  public static AJEntityCoreContent fetchContent(String questionId, String collectionId){
      DB coreDb = new DB("coreDb");
      try {
        coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
        coreDb.openTransaction();
        LazyList<AJEntityCoreContent> results = AJEntityCoreContent
            .where(FETCH_CONTENT, questionId, collectionId, false);
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
