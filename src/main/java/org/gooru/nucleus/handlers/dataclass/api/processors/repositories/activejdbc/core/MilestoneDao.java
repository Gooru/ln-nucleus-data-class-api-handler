package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.core;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.dataclass.api.app.components.DataSourceRegistry;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityMilestoneLessonMap;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mukul@gooru
 *  
 */
public class MilestoneDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(MilestoneDao.class);
	private DB coreDb;

	public LazyList<AJEntityMilestoneLessonMap> fetchMilestoneLessonIds(String milestoneId, Boolean readOnly) {
		coreDb = new DB("coreDb");
		try {
			coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
			if (readOnly) {
				Base.connection().setReadOnly(true);
			}
			coreDb.openTransaction();		    
	        LazyList<AJEntityMilestoneLessonMap> lessonIdList = AJEntityMilestoneLessonMap.findBySQL
	        		(AJEntityMilestoneLessonMap.FETCH_MILESTONE_LESSON_IDS, milestoneId);		    
			coreDb.commitTransaction();
			return lessonIdList;
		} catch (Throwable throwable) {
			coreDb.rollbackTransaction();
			return null;
		} finally {
			coreDb.close();
		}
	}

	public void fetchMilestoneUnitIds(String milestoneId, Boolean readOnly) {

		coreDb = new DB("coreDb");
		try {
			coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
			if (readOnly) {
				Base.connection().setReadOnly(true);
			}
			coreDb.openTransaction();

			coreDb.commitTransaction();
		} catch (Throwable throwable) {
			coreDb.rollbackTransaction();
		} finally {
			coreDb.close();
		}
	}

}
