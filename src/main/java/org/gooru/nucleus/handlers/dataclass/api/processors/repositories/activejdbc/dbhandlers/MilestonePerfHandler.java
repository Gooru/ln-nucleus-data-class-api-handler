package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters.ResponseAttributeIdentifier;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityMilestoneLessonMap;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters.ValueMapper;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 * 
 */
public class MilestonePerfHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(MilestonePerfHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
	private static final String FRAMEWORK_CODE = "fwCode";
	private static final String REQUEST_USERID = "userUid";
	int totalCount = 0;
	private final ProcessorContext context;
	private String collectionType;
	private String fwCode;

	public MilestonePerfHandler(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public ExecutionResult<MessageResponse> checkSanity() {
		if (context.request() == null || context.request().isEmpty()) {
			LOGGER.warn("Invalid request received to fetch Student Performance for Milestones");
			return new ExecutionResult<>(
					MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance for Milestones"),
					ExecutionStatus.FAILED);
		} else if (context.request() != null && !context.request().isEmpty()) {
	          this.collectionType = this.context.request().getString(REQUEST_COLLECTION_TYPE);
	          if (StringUtil.isNullOrEmpty(collectionType)) {
	              LOGGER.warn("CollectionType is mandatory to fetch Student Performance in Milestones");
	              return new ExecutionResult<>(
	                      MessageResponseFactory.createInvalidRequestResponse("CollectionType Missing. Cannot fetch Student Performance in Milestones"),
	                      ExecutionStatus.FAILED);
	            }
	          
	          this.fwCode = this.context.request().getString(FRAMEWORK_CODE);
	          if (StringUtil.isNullOrEmpty(fwCode)) {
	              LOGGER.warn("Framework Code is mandatory to fetch Student Performance in Milestones");
	              return new ExecutionResult<>(
	                      MessageResponseFactory.createInvalidRequestResponse("Framework Code Missing. Cannot fetch Student Performance in Milestones"),
	                      ExecutionStatus.FAILED);
	            }    	  
	      }
		LOGGER.debug("checkSanity() OK");
		return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public ExecutionResult<MessageResponse> validateRequest() {
		if (context.getUserIdFromRequest() == null
				|| (context.getUserIdFromRequest() != null && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
			LOGGER.debug("User ID in the session : {}", context.userIdFromSession());
			LOGGER.debug("User ID in the request : {}", context.getUserIdFromRequest());
			List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, this.context.classId(), this.context.userIdFromSession());
			if (owner.isEmpty()) {
				LOGGER.debug("validateRequest() FAILED");
				return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
			}
		}
		LOGGER.debug("validateRequest() OK");
		return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public ExecutionResult<MessageResponse> executeRequest() {
		JsonObject resultBody = new JsonObject();
		JsonArray resultarray = new JsonArray();
		Map<String, Integer> milestoneAssessmentCountMap = new HashMap<String, Integer>();
		Map<String, Integer> milestoneCollectionCountMap = new HashMap<String, Integer>();

		String userId = this.context.request().getString(REQUEST_USERID);
        String addCollTypeFilterToQuery = AJEntityBaseReports.ADD_COLL_TYPE_FILTER_TO_QUERY;
        if (!this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
        	addCollTypeFilterToQuery = AJEntityBaseReports.ADD_ASS_TYPE_FILTER_TO_QUERY;
        }

		List<String> milestoneIds = new ArrayList<>();
		List<String> userIds = new ArrayList<>();

		if (context.classId() != null && StringUtil.isNullOrEmpty(userId)) {
			LOGGER.info("UserID is not in the request fetch Student Performance in Milestone, Assume user is a teacher");
			LazyList<AJEntityBaseReports> userIDforCourse = AJEntityBaseReports.findBySQL(AJEntityBaseReports.SELECT_DISTINCT_USERID_FOR_COURSE_ID + addCollTypeFilterToQuery,
					context.classId(), context.courseId());
			userIDforCourse.forEach(u -> userIds.add(u.getString(AJEntityBaseReports.GOORUUID)));
		} else {
			userIds.add(userId);
		}

		//GET DISTINCT(MILESTONES) FOR THIS COURSE	        
		LazyList<AJEntityMilestoneLessonMap> milestoneIDforCourse;
		milestoneIDforCourse = AJEntityMilestoneLessonMap.findBySQL(AJEntityMilestoneLessonMap.SELECT_DISTINCT_MILESTONE_ID_FOR_COURSE,
				UUID.fromString(context.courseId()), fwCode);
		
		if (!milestoneIDforCourse.isEmpty()) {
			milestoneIDforCourse.forEach(milestone -> milestoneIds.add(milestone.getString(AJEntityMilestoneLessonMap.MILESTONE_ID)));
		}
		
		for (String mileId : milestoneIds) {
			List<Map> assessmentPerf;
			List<String> lessonIds = new ArrayList<>();
			//GET LESSONS FOR EACH MILESTONE
			LazyList<AJEntityMilestoneLessonMap> lessonIDforMilestone;
			lessonIDforMilestone = AJEntityMilestoneLessonMap.findBySQL(AJEntityMilestoneLessonMap.SELECT_DISTINCT_LESSON_ID_FOR_MILESTONE_ID,
					UUID.fromString(context.courseId()), mileId, fwCode);
			lessonIDforMilestone.forEach(lesson -> lessonIds.add(lesson.getString(AJEntityMilestoneLessonMap.LESSON_ID)));

			if (!lessonIDforMilestone.isEmpty()) {				
			for (String userID : userIds) {
				JsonObject contentBody = new JsonObject();
				JsonArray milestonePerfArray = new JsonArray();
				//List<Map> lessonDataList;
				if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
					assessmentPerf = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_MILESTONE_PERF_FOR_COLLECTION, context.classId(), context.courseId(),
							userID, listToPostgresArrayString(lessonIds));
				} else{
					assessmentPerf = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_MILESTONE_PERF_FOR_ASSESSMENT, context.classId(), context.courseId(),
							userID, listToPostgresArrayString(lessonIds), EventConstants.COLLECTION_PLAY);
				}

				if (!assessmentPerf.isEmpty()) {
					assessmentPerf.forEach(m -> {
						List<Map> scoreCompletionMap;
						List<Map> scoreMap = null;

						if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
							scoreCompletionMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_COLL_COMP_COUNT_SCORE_FOREACH_MILESTONE_ID, context.classId(), context.courseId(),
									listToPostgresArrayString(lessonIds), userID, EventConstants.COLLECTION_PLAY);
							scoreMap = Base.findAll(AJEntityBaseReports.GET_COLL_SCORE_FOREACH_MILESTONE_ID, context.classId(), context.courseId(),
									listToPostgresArrayString(lessonIds), userID);
						} else {
							scoreCompletionMap = Base.findAll(AJEntityBaseReports.GET_COMPLETED_ASMT_COMP_COUNT_SCORE_FOREACH_MILESTONE_ID, context.classId(),
									context.courseId(), listToPostgresArrayString(lessonIds), userID, EventConstants.COLLECTION_PLAY);
						}
						JsonObject mileData = ValueMapper.map(ResponseAttributeIdentifier.getMilestonePerformanceAttributesMap(), m);
						scoreCompletionMap.forEach(sc -> {
							mileData.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
									Integer.valueOf(sc.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
							mileData.put(AJEntityBaseReports.ATTR_SCORE, sc.get(AJEntityBaseReports.ATTR_SCORE) != null ?
									Math.round(Double.valueOf(sc.get(AJEntityBaseReports.ATTR_SCORE).toString())) : null);
						});

						if(scoreMap != null && !scoreMap.isEmpty() && this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
							scoreMap.forEach(score ->{
								double maxScore = Double.valueOf(score.get(AJEntityBaseReports.MAX_SCORE).toString());
								if(maxScore > 0 && (score.get(AJEntityBaseReports.SCORE) != null)) {
									double sumOfScore = Double.valueOf(score.get(AJEntityBaseReports.SCORE).toString());                	
									mileData.put(AJEntityBaseReports.ATTR_SCORE, (Math.round((sumOfScore / maxScore) * 100)));
								}else {
									mileData.putNull(AJEntityBaseReports.ATTR_SCORE);
								}
							});
						}


						if (this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
							mileData.put(EventConstants.VIEWS, mileData.getInteger(EventConstants.ATTEMPTS));
							mileData.remove(EventConstants.ATTEMPTS);
							
							if (milestoneCollectionCountMap.containsKey(mileId)) {
								totalCount = milestoneCollectionCountMap.get(mileId);
							} else {
								Object classTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_MILESTONE_COLLECTION_COUNT,
										context.courseId(), listToPostgresArrayString(lessonIds));
								totalCount = classTotalCount != null ? (Integer.valueOf(classTotalCount.toString())) : 0;
								milestoneCollectionCountMap.put(mileId, totalCount);
							}
							mileData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount);
							
						} else if (this.collectionType.equalsIgnoreCase(EventConstants.ASSESSMENT)) {							
							if (milestoneAssessmentCountMap.containsKey(mileId)) {
								totalCount = milestoneAssessmentCountMap.get(mileId);
							} else {
								Object classTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_MILESTONE_ASSESSMENT_COUNT,
										context.courseId(), listToPostgresArrayString(lessonIds));
								totalCount = classTotalCount != null ? (Integer.valueOf(classTotalCount.toString())) : 0;
								milestoneAssessmentCountMap.put(mileId, totalCount);
							}
							mileData.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, totalCount);
						}
						milestonePerfArray.add(mileData);
					});
				} else {
					LOGGER.info("No data returned for Student Perf in this milestone");
				}
				// Form the required Json pass it on
				contentBody.put(JsonConstants.USAGE_DATA, milestonePerfArray).put(JsonConstants.USERUID, userID).put(JsonConstants.MILESTONE_ID, mileId);
				resultarray.add(contentBody);
			}
		}//lessons for milestones are empty
		}
		resultBody.put(JsonConstants.CONTENT, resultarray);
		return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
	}

	@Override
	public boolean handlerReadOnly() {
		return true;
	}


	private String listToPostgresArrayString(List<String> input) {
		int approxSize = ((input.size() + 1) * 36); //UUID = 36chars
		Iterator<String> it = input.iterator();
		if (!it.hasNext()) {
			return "{}";
		}

		StringBuilder sb = new StringBuilder(approxSize);
		sb.append('{');
		for (;;) {
			String s = it.next();
			sb.append('"').append(s).append('"');
			if (!it.hasNext()) {
				return sb.append('}').toString();
			}
			sb.append(',');
		}
	}


}
