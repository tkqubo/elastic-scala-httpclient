package jp.co.bizreach.elasticsearch4s

import jp.co.bizreach.elasticsearch4s._
import org.elasticsearch.index.query.{QueryBuilders, MatchQueryBuilder, MultiMatchQueryBuilder}

object Sample extends App {
  val keyword : String = "test"
  val location: String = null

  ESClient.using("http://localhost:9200") { client =>
    implicit val config = ESConfig("stanby_job", "job")

//    client.insert(config, new models.Job(
//      closeDate = org.joda.time.LocalDateTime.now(),
//      jobThumbnail = None,
//      RegTimestamp = None,
//      lastModified = None,
//      companyAddress  = "test",
//      jobApplicationGuidelines = "test",
//      jobHtmlBody = "test",
//      indexType = None,
//      employmentStatus = None,
//      parentUrl = None,
//      salary = None,
//      limitAge = None,
//      informationSource = "test",
//      companyName = "test",
//      signUpBonus = None,
//      jobContent = "test",
//      dispWorkLocation = None,
//      examinationDate = org.joda.time.LocalDate.now(),
//      openDate = org.joda.time.LocalDate.now(),
//      workingTime = None,
//      workLocation = None,
//      jobTitle = "test",
//      UpdTimestamp = None,
//      crawlUrl = None
//    ))

    println(client.list[models.Job](config) { searcher =>
      // クエリ
      searcher.setQuery(QueryBuilders.termQuery("_id", "1"))
//      val q = QueryBuilders.boolQuery()
//      Option(keyword).filter(_.nonEmpty).foreach(k => {
//        q.must(new MultiMatchQueryBuilder(k, "jobHtmlBody").analyzer("kuromoji_analyzer").operator(MatchQueryBuilder.Operator.AND).boost(1.0f))
//      })
//      Option(location).filter(_.nonEmpty).foreach(l => {
//        q.must(new MultiMatchQueryBuilder(l, "workLocation").analyzer("kuromoji_analyzer").operator(MatchQueryBuilder.Operator.AND).boost(1.0f))
//      })
//      searcher.setQuery(q)
      // ハイライト
      searcher.addHighlightedField("jobHtmlBody")
    })

    client.delete(config, "123")

    println("333")

    client.release()
  }

}
