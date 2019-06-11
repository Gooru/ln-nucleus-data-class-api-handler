package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */

public interface ReportRepo {

  MessageResponse getStudentPeersInCourse();

  MessageResponse getStudentPeersInUnit();

  MessageResponse getStudentPeersInLesson();

  MessageResponse getStudentCurrentLocation();

  MessageResponse getStudentPerformanceInCourse();

  MessageResponse getStudentPerformanceInUnit();

  MessageResponse getStudentPerformanceInLesson();

  MessageResponse getStudentPerformanceInAssessment();

  MessageResponse getStudentPerformanceInCollection();

  MessageResponse getStudentSummaryInCollection();

  MessageResponse getStudentSummaryInAssessment();

  MessageResponse getSessionStatus();

  MessageResponse getUserAssessmentSessions();

  MessageResponse getUserCollectionSessions();

  MessageResponse getSessionWiseTaxonmyReport();

  MessageResponse getAllStudentPerfInCourse();

  MessageResponse getAllStudentPerfInUnit();

  MessageResponse getStudentPerfInAllClasses();

  MessageResponse getStudentLocationInAllClasses();

  MessageResponse getStudPerfMultipleCollections();

  MessageResponse getStudPerfCourseCollections();

  MessageResponse getStudPerfCourseAssessments();

  MessageResponse getLearnerPerformanceInCourse();

  MessageResponse getLearnerPerformanceInUnit();

  MessageResponse getLearnerPerformanceInLesson();

  MessageResponse getLearnerPerformanceInAssessment();

  MessageResponse getLearnerPerformanceInIndependentAssessment();

  MessageResponse getIndependentLearnerLocation();

  MessageResponse getIndependentLearnerPerformance();

  MessageResponse getIndLearnerCoursesLocation();

  MessageResponse getIndLearnerAssessmentsLocation();

  MessageResponse getIndLearnerCollectionsLocation();

  MessageResponse getIndLearnerAllCoursesPerf();

  MessageResponse getIndLearnerIndAssessmentsPerf();

  MessageResponse getIndLearnerIndCollectionsPerf();

  MessageResponse getIndLearnerCourseCollectionsPerf();

  MessageResponse getIndLearnerCourseAssessmentsPerf();

  MessageResponse getIndLearnerTaxSubjects();

  MessageResponse getLearnerCourses();

  MessageResponse getIndLearnerSummaryInCollection();

  MessageResponse getIndLearnerSummaryInAssessment();

  MessageResponse getIndLearnerAssessmentSessions();

  MessageResponse getIndLearnerCollectionSessions();

  // Rubric Grading
  MessageResponse getRubricQuesToGrade();

  MessageResponse getStudentsForRubricQuestion();

  MessageResponse getStudentAnswersForRubricQuestion();

  MessageResponse getRubricSummaryforQuestion();

  // DCA
  MessageResponse getStudPerfDailyClassActivity();

  MessageResponse getStudentSummaryInDCACollection();

  MessageResponse getStudentSummaryInDCAAssessment();

  MessageResponse getDCASessionTaxonomyReport();

  MessageResponse getStudentPerformanceInDCAAssessment();

  MessageResponse getStudentPerformanceInDCACollection();

  MessageResponse getDCAMonthlyTeacherReport();

  MessageResponse getStudentDCAAssessmentSessions();

  MessageResponse getDCAClassPerformance();

  MessageResponse getDCAAllClassesPerformance();

  // NU
  MessageResponse getDataReports();

  MessageResponse getCoursesComptencyCompletion();

  // ATC
  MessageResponse getStudentPerfVsCompletion();

  MessageResponse getStudentCourseAllItemsReport();

  MessageResponse getIndependentLearnerCourseAllItemsReport();

  MessageResponse getDCAMonthlyClassSummaryHandler();

  MessageResponse getDCAClassSummaryForMonthHandler();

  MessageResponse getDCAActivityAllStudentSummaryReportHandler();

  MessageResponse getStudentCAAssessmentSessionPerformance();

  MessageResponse getStudentCACollectionSessionPerformance();

  // Milestone
  MessageResponse getStudentMilestoneLessonPerfHandler();

  MessageResponse getStudentMilestonePerfHandler();

  // Milestone - Independent Learner
  MessageResponse getILMilestoneLessonPerfHandler();

  MessageResponse getILMilestonePerfHandler();

  // DCA Rubric Grading
  MessageResponse getDCARubricQuesToGrade();

  MessageResponse getDCAStudentsForRubricQuestion();

  MessageResponse getDCAStudentAnswersForRubricQuestion();

  MessageResponse getDCARubricSummaryforQuestion();

  // DCA OA Grading
  MessageResponse getDCAOAToGrade();
  
  MessageResponse getDCAOAToGradeStudent();

  MessageResponse getDCAStudentsForOA();
  
  MessageResponse getDCAStudentSubmissionsForOA();

  MessageResponse getDCAStudOAPerformance();

}
