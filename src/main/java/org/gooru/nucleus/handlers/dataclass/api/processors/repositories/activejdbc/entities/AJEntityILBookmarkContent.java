package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;


/**
 * @author mukul@gooru
 * 
 */
@Table("learner_bookmarks")
public class AJEntityILBookmarkContent extends Model {
	
	  public static final String ID = "id";
	  public static final String USER_ID = "user_id";
	  public static final String CONTENT_ID = "content_id";
	  public static final String CONTENT_TYPE = "content_type";
	  public static final String TITLE = "title";	  
	  public static final String UPDATED_AT = "updated_at";
	  
	  public static final String ATTR_COURSE = "course";
	  public static final String ATTR_ASSESSMENT = "assessment";
	  public static final String ATTR_COLLECTION = "collection";
	  
	  
	    //Independent Learner Performance and Location Cards	    
	    public static final String SELECT_DISTINCT_IL_CONTENTID =
	            "SELECT DISTINCT(content_id) FROM learner_bookmarks "
	            + "WHERE user_id = ? AND content_type = ?";

}
