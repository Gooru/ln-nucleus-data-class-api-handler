
Class Reports READ Handler
===========================

Currently this will serve the following Reports for Students and Teachers.

Student Reports:

    GetStudentPeersinCourse (getCoursePeers)
	/api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/peers

    GetStudentPeersinUnit (getUnitPeers)
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/peers
    
    GetStudentPeersinLesson (getLessonPeers)
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/peers
    
    GetCurrentLocation (getUserCurrentLocationInLesson)
    /api/nucleus-insights/v2/class/{classGooruId}/user/{userUid}/current/location
    
    Get Student Course Performance (getCoursePeformance)    
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/performance
    
    Get Student Performance In Unit (getUnitPeformance)
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/performance
    
    Get Student Performance In Lesson (getLessonPerformance)
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGoouId}/performance
    
    Get Student Performance In Collection/Assessment (getCollectionPeformance)
    /api/nucleus-insights/v2/{collectionType}/{contentGooruId}/user/{userUid}
    
    Get Session Status (getSessionStatus)
    /api/nucleus-insights/v2/collection/{contentGooruId}/session/{sessionId}/status

    GET ALL USER SESSIONS
    /api/nucleus-insights/v2/{collectionType}/{contentGooruId}/sessions    

Teacher Reports:
    
    Get All Students Performance In Course
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/performance
    
    Get All Students Performance In Collection/Assessment
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/
    {collectionType}/{contentGooruId}/performance    
    
    Get All Students Performance In Unit
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/performance
    
    Get All Students Performance In Lesson
    /api/nucleus-insights/v2/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/performance
