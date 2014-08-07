package models

import Job._

class Job(val closeDate: org.joda.time.LocalDateTime, val jobThumbnail: Option[JobThumbnail], val RegTimestamp: Option[org.joda.time.LocalDateTime], val lastModified: Option[org.joda.time.LocalDateTime], val companyAddress: String, val jobApplicationGuidelines: String, val jobHtmlBody: String, val indexType: Option[String], val employmentStatus: Option[Array[String]], val parentUrl: Option[String], val salary: Option[Salary], val limitAge: Option[Long], val informationSource: String, val companyName: String, val signUpBonus: Option[SignUpBonus], val jobContent: String, val dispWorkLocation: Option[String], val examinationDate: org.joda.time.LocalDate, val openDate: org.joda.time.LocalDate, val workingTime: Option[String], val workLocation: Option[String], val jobTitle: String, val UpdTimestamp: Option[org.joda.time.LocalDateTime], val crawlUrl: Option[String])

object Job {

  val typeName = "job"

  case class JobThumbnail(orgJpg: Option[String], orgUrl: Option[String], `120x120`: Option[String], requestUrl: Option[String])
  case class Salary(annualSalaryConversion: Option[AnnualSalaryConversion], dispSalary: Option[String], dispSalaryOrg: Option[String], dailyWage: Option[DailyWage], monthlySalary: Option[MonthlySalary], hourlyWage: Option[HourlyWage], annualSalary: Option[AnnualSalary])
  case class AnnualSalaryConversion(min: Option[Long], max: Option[Long], average: Option[Long])
  case class DailyWage(min: Option[Long], max: Option[Long], average: Option[Long])
  case class MonthlySalary(min: Option[Long], max: Option[Long], average: Option[Long])
  case class HourlyWage(min: Option[Long], max: Option[Long], average: Option[Long])
  case class AnnualSalary(min: Option[Long], max: Option[Long], average: Option[Long])
  case class SignUpBonus(dispBonus: Option[String], bonusAmount: Option[BonusAmount], isBonus: Option[String])
  case class BonusAmount(min: Option[Long], max: Option[Long], average: Option[Long])

  val closeDate = "closeDate"
  val jobThumbnail = JobThumbnailNames
  object JobThumbnailNames {
    val orgJpg = "jobThumbnail.orgJpg"
    val orgUrl = "jobThumbnail.orgUrl"
    val `120x120` = "jobThumbnail.120x120"
    val requestUrl = "jobThumbnail.requestUrl"
    override def toString() = "jobThumbnail"
  }
  val RegTimestamp = "RegTimestamp"
  val lastModified = "lastModified"
  val companyAddress = "companyAddress"
  val jobApplicationGuidelines = "jobApplicationGuidelines"
  val jobHtmlBody = "jobHtmlBody"
  val indexType = "indexType"
  val employmentStatus = "employmentStatus"
  val parentUrl = "parentUrl"
  val salary = SalaryNames
  object SalaryNames {
    val annualSalaryConversion = AnnualSalaryConversionNames
    object AnnualSalaryConversionNames {
      val min = "salary.annualSalaryConversion.min"
      val max = "salary.annualSalaryConversion.max"
      val average = "salary.annualSalaryConversion.average"
        override def toString() = "salary.annualSalaryConversion"
    }
    val dispSalary = "salary.dispSalary"
    val dispSalaryOrg = "salary.dispSalaryOrg"
    val dailyWage = DailyWageNames
    object DailyWageNames {
      val min = "salary.dailyWage.min"
      val max = "salary.dailyWage.max"
      val average = "salary.dailyWage.average"
        override def toString() = "salary.dailyWage"
    }
    val monthlySalary = MonthlySalaryNames
    object MonthlySalaryNames {
      val min = "salary.monthlySalary.min"
      val max = "salary.monthlySalary.max"
      val average = "salary.monthlySalary.average"
        override def toString() = "salary.monthlySalary"
    }
    val hourlyWage = HourlyWageNames
    object HourlyWageNames {
      val min = "salary.hourlyWage.min"
      val max = "salary.hourlyWage.max"
      val average = "salary.hourlyWage.average"
        override def toString() = "salary.hourlyWage"
    }
    val annualSalary = AnnualSalaryNames
    object AnnualSalaryNames {
      val min = "salary.annualSalary.min"
      val max = "salary.annualSalary.max"
      val average = "salary.annualSalary.average"
        override def toString() = "salary.annualSalary"
    }
    override def toString() = "salary"
  }
  val limitAge = "limitAge"
  val informationSource = "informationSource"
  val companyName = "companyName"
  val signUpBonus = SignUpBonusNames
  object SignUpBonusNames {
    val dispBonus = "signUpBonus.dispBonus"
    val bonusAmount = BonusAmountNames
    object BonusAmountNames {
      val min = "signUpBonus.bonusAmount.min"
      val max = "signUpBonus.bonusAmount.max"
      val average = "signUpBonus.bonusAmount.average"
        override def toString() = "signUpBonus.bonusAmount"
    }
    val isBonus = "signUpBonus.isBonus"
    override def toString() = "signUpBonus"
  }
  val jobContent = "jobContent"
  val dispWorkLocation = "dispWorkLocation"
  val examinationDate = "examinationDate"
  val openDate = "openDate"
  val workingTime = "workingTime"
  val workLocation = "workLocation"
  val jobTitle = "jobTitle"
  val UpdTimestamp = "UpdTimestamp"
  val crawlUrl = "crawlUrl"

}