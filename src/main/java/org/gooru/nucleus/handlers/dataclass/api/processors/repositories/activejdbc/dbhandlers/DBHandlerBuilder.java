package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;

/**
 * Created by mukul@gooru
 */

public final class DBHandlerBuilder {

    private DBHandlerBuilder() {
        throw new AssertionError();
    }

    public static DBHandler buildStudentPeersInCourseHandler(ProcessorContext context) {
        return new StudentPeersInCourseHandler(context);
    }

    public static DBHandler buildStudentPeersInUnitHandler(ProcessorContext context) {
        return new StudentPeersInUnitHandler(context);
    }
    
    public static DBHandler buildStudentPeersInLessonHandler(ProcessorContext context) {
        return new StudentPeersInLessonHandler(context);
    }

    public static DBHandler buildStudentCurrentLocationHandler(ProcessorContext context) {
        return new StudentCurrentLocationHandler(context);
    }
    
    public static DBHandler buildStudentCoursePerfHandler(ProcessorContext context) {
        return new StudentCoursePerfHandler(context);
    }
    
    public static DBHandler buildStudentUnitPerfHandler(ProcessorContext context) {
        return new StudentUnitPerfHandler(context);
    }
    
    public static DBHandler buildStudentLessonPerfHandler(ProcessorContext context) {
        return new StudentLessonPerfHandler(context);
    }
    
    public static DBHandler buildStudentAssessmentPerfHandler(ProcessorContext context) {
      return new StudentAssessmentPerfHandler(context);
    }
    
    public static DBHandler buildStudentCollectionSummaryHandler(ProcessorContext context) {
        return new StudentCollectionSummaryHandler(context);
    }
    
    public static DBHandler buildStudentAssessmentSummaryHandler(ProcessorContext context) {
        return new StudentAssessmentSummaryHandler(context);
    }
    
    public static DBHandler buildSessionStatusHandler(ProcessorContext context) {
        return new SessionStatusHandler(context);
    }
    
    public static DBHandler buildUserAssessmentSessionsHandler(ProcessorContext context) {
        return new UserAssessmentSessionsHandler(context);
    }
    
    public static DBHandler buildUserCollectionSessionsHandler(ProcessorContext context) {
        return new UserCollectionSessionsHandler(context);
    }
    
    public static DBHandler buildAllStudentCoursePerfHandler(ProcessorContext context) {
        return new AllStudentCoursePerfHandler(context);
    }
    
    public static DBHandler buildAllStudentUnitPerfHandler(ProcessorContext context) {
        return new AllStudentUnitPerfHandler(context);
    }

    public static DBHandler buildSessionTaxonomyReportHandler(ProcessorContext context) {
      return new SessionTaxonomyReportHandler(context);
    }
    
    public static DBHandler buildStudentPerfInAllClasses(ProcessorContext context) {
      return new StudentPerfInAllClasses(context);
    }
    
    public static DBHandler buildStudentLocAllClassesHandler(ProcessorContext context) {
        return new StudentLocationAllClassesHandler(context);
      }
    
    
    public static DBHandler buildStudPerfDailyClassActivityHandler(ProcessorContext context) {
        return new StudPerfDailyActivityHandler(context);
      }

    public static DBHandler buildStudPerfMultipleCollectionHandler(ProcessorContext context) {
        return new StudPerfMultipleCollectionHandler(context);
      }
        
    public static DBHandler buildStudPerfCourseCollectionHandler(ProcessorContext context) {
        return new StudPerfCourseCollectionHandler(context);
      }
    
    public static DBHandler buildStudPerfCourseAssessmentHandler(ProcessorContext context) {
        return new StudPerfCourseAssessmentHandler(context);
      }
    
    public static DBHandler buildLearnerCoursePerfHandler(ProcessorContext context) {
      return new IndependentLearnerCoursePerfHandler(context);
    }
  
    public static DBHandler buildLearnerUnitPerfHandler(ProcessorContext context) {
      return new IndependentLearnerUnitPerfHandler(context);
    }
  
    public static DBHandler buildLearnerLessonPerfHandler(ProcessorContext context) {
      return new IndependentLearnerLessonPerfHandler(context);
    }
  
    public static DBHandler buildLearnerAssessmentPerfHandler(ProcessorContext context) {
      return new IndependentLearnerAssessmentPerfHandler(context);
    }
  
    public static DBHandler buildLearnerIndependentAssessmentPerfHandler(ProcessorContext context) {
      return new IndependentLearnerIndependentAssessmentPerfHandler(context);
    }
    
    public static DBHandler buildIndependentLearnerCourses(ProcessorContext context) {
      return new IndependentLearnerCoursesHandler(context);
    }
    
    public static DBHandler buildIndLearnerCoursesLocationHandler(ProcessorContext context) {
        return new IndLearnerAllCoursesLocationHandler(context);
      }
    
    public static DBHandler buildIndLearnerAssessmentsLocationHandler(ProcessorContext context) {
        return new IndLearnerAllIndAssessmentLocHandler(context);
      }
    
    public static DBHandler buildIndLearnerCollectionsLocationHandler(ProcessorContext context) {
        return new IndLearnerAllIndCollectionLocHandler(context);
      }
    
    public static DBHandler buildIndLearnerCourseCollectionsPerfHandler(ProcessorContext context) {
        return new IndLearnerCourseCollectionsPerfHandler(context);
      }
    
    public static DBHandler buildIndLearnerCourseAssessmentssPerfHandler(ProcessorContext context) {
        return new IndLearnerCourseAssessmentsPerfHandler(context);
      }
    
}
