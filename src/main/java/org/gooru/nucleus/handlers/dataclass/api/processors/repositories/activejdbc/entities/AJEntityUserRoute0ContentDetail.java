package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.gooru.nucleus.handlers.dataclass.api.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.LazyList;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author renuka.
 */
@DbName("coreDb")
@Table("user_route0_content_detail")
public class AJEntityUserRoute0ContentDetail extends Model {
    
    public static final String LESSON_TITLE = "lesson_title";
    
    public static final String UNIT_TITLE = "unit_title";

    public static AJEntityUserRoute0ContentDetail fetchRoute0SuggestedContent(String collId) throws Throwable {
        DB coreDb = new DB("coreDb");
        try {
          coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
          coreDb.connection().setReadOnly(true);
          coreDb.openTransaction();
          LazyList<AJEntityUserRoute0ContentDetail> results = AJEntityUserRoute0ContentDetail
              .find("collection_id = ?::uuid", collId);
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
