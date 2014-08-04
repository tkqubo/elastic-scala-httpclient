package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.facet.{Facet, FacetBuilder}
import org.elasticsearch.action.index.IndexResponse
import ESClient._
import org.elasticsearch.search.highlight.HighlightField
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
 * Helper for accessing to Elasticsearch.
 *
 * Created by nishiyama on 2014/04/25.
 */
object ESClient {

  val logger = LoggerFactory.getLogger(classOf[ESClient])

  /**
   * Create a ESSearchHelper instance.
   */
  def apply(clusterName: String, transportAddress: (String, Int)*): ESClient = {
    val setting = ESSetting(clusterName, transportAddress: _*)
    logger.debug("******** ESSetting:" + setting.toString)
    var client: TransportClient = null
    try {
      client = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", setting.clusterName).build())
      setting.transportAddress.foreach(t => {
        client.addTransportAddress(new InetSocketTransportAddress(t._1, t._2))
      })
      new ESClient(client)
    } catch {
      case e: Exception => {
        if (client != null) client.close()
        throw e
      }
    }
  }

  /**
   * This is the entry point of processing using ElasticSearch.
   * Give ESConfig and your function which takes ESSearchHelper as an argument.
   */
  def withElasticSearch[T](clusterName: String, transportAddress: (String, Int)*)(f: ESClient => T): T = {
    val helper = ESClient(clusterName, transportAddress: _*)
    try {
      f(helper)
    } finally {
      helper.release()
    }
  }

  private case class ESSetting(clusterName: String, transportAddress: (String, Int)*)

  case class ESConfig(indexName: String, typeName: String)
  case class ESHighlightFragment(name: String, fragmentSize:Int = -1, numberOfFragments:Int = -1)

  case class ESSearchResult[T](totalHits: Long, list: List[ESSearchResultItem[T]], facets: Map[String, Facet])
  case class ESSearchResultItem[T](id: String, doc: T, highlightFields: Map[String, HighlightField])

}

class ESClient(client: TransportClient) {
  private var searcher: SearchRequestBuilder = null

  def boolQuery(q: (BoolQueryBuilder) => BoolQueryBuilder): ESClient = {
    searcher.setQuery(q(QueryBuilders.boolQuery()))
    this
  }

  def fieldSorts(fieldSorts: (String, SortOrder)*): ESClient = {
    fieldSorts.foreach(f => this.searcher.addSort(f._1, f._2))
    this
  }

  def highlighterTag(preTag: String, postTag:String) :ESClient = {
    this.searcher.setHighlighterPreTags(preTag)
    this.searcher.setHighlighterPostTags(postTag)
    this
  }

  def highlighterFields(fields: ESHighlightFragment*) :ESClient = {
    fields.foreach {
      case ESHighlightFragment(name, -1, -1) => this.searcher.addHighlightedField(name)
      case ESHighlightFragment(name, size, -1) => this.searcher.addHighlightedField(name, size)
      case f => this.searcher.addHighlightedField(f.name, f.fragmentSize, f.numberOfFragments)
    }
    this
  }

  def facets(facets: FacetBuilder*) :ESClient = {
    facets.foreach(this.searcher.addFacet)
    this
  }

  def explain(b: Boolean) :ESClient = {
    this.searcher.setExplain(b)
    this
  }

  def paging(from: Int, size: Int) = {
    this.searcher.setFrom(from).setSize(size)
  }

  def insertJson(config: ESConfig)(json: String):IndexResponse = {
    logger.debug(s"insertJson:\n${json}")
    val indexer = client.prepareIndex(config.indexName, config.typeName).setSource(json)
    logger.debug(s"insertRequest:\n${indexer.toString}")
    indexer.execute.actionGet
  }

  def insert(config: ESConfig)(entity: AnyRef):IndexResponse = {
    insertJson(config)(JsonUtils.serialize(entity))
  }

  def updateJson(config: ESConfig)(id: String, json: String):IndexResponse = {
    logger.debug(s"updateJson:\n${json}")
    val indexer = client.prepareIndex(config.indexName, config.typeName, id).setSource(json)
    logger.debug(s"updateRequest:\n${indexer.toString}")
    indexer.execute.actionGet
  }

  def update(config: ESConfig)(id: String, entity: AnyRef):IndexResponse = {
    updateJson(config)(id, JsonUtils.serialize(entity))
  }

  def delete(setting: ESSetting)(config: ESConfig)(id: String) = {
    logger.debug(s"delete id:\n${id}")
    val deleter = client.prepareDelete(config.indexName, config.typeName, id)
    logger.debug(s"deleteRequest:\n${deleter.toString}")
    deleter.execute.actionGet
  }

  // TODO 引数にはsearcherを直接渡す？
  def search(config: ESConfig)(f: ESClient => Unit):SearchResponse = {
    logger.debug("******** ESConfig:" + config.toString)
    searcher = client.prepareSearch(config.indexName).setTypes(config.typeName)
    f(this)
    logger.debug(s"searchRequest:${searcher.toString}")
    logger.debug(searcher.toString)
    searcher.execute.actionGet
  }

  // TODO 引数にはsearcherを直接渡す？
  def find[T](config: ESConfig, clazz: Class[T])(f: ESClient => Unit): Option[(String, T)] = {
    logger.debug("******** ESConfig:" + config.toString)
    searcher = client.prepareSearch(config.indexName).setTypes(config.typeName)
    f(this)
    logger.debug(s"searchRequest:${searcher.toString}")
    logger.debug(searcher.toString)
    val response = searcher.execute.actionGet
    val hits = response.getHits.getHits
    if(hits.length == 0){
      None
    } else {
      Some((hits.head.getId, JsonUtils.deserialize(hits.head.getSourceAsString, clazz)))
    }
  }

  // TODO 引数にはsearcherを直接渡す？
  def list[T](config: ESConfig, clazz: Class[T])(f: ESClient => Unit): ESSearchResult[T] = {
    logger.debug("******** ESConfig:" + config.toString)
    searcher = client.prepareSearch(config.indexName).setTypes(config.typeName)
    f(this)
    logger.debug(s"searchRequest:${searcher.toString}")
    logger.debug(searcher.toString)
    val response = searcher.execute.actionGet
    ESSearchResult(
      response.getHits.getTotalHits,
      response.getHits.getHits.map { hit =>
        ESSearchResultItem(hit.getId, JsonUtils.deserialize(hit.getSourceAsString, clazz), hit.getHighlightFields.asScala.toMap)
      }.toList,
      if(response.getFacets != null) response.getFacets.facetsAsMap.asScala.toMap else Map.empty[String, Facet]
    )
  }


  def release() = client.close()

}
