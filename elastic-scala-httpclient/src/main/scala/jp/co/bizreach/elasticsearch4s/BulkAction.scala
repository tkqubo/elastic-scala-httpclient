package jp.co.bizreach.elasticsearch4s

import JsonUtils._

sealed trait BulkAction {
  def jsonString: String
}

object BulkAction {

  case class Create(config: ESConfig, doc: AnyRef, id: Option[String] = None) extends BulkAction {
    def jsonString: String = {
      s"""{ "create" : { "_index" : "${config.indexName}", "_type" : "${config.typeName.getOrElse("")}"${id.map(_id => s""", "_id": "${_id}"""").getOrElse("")} } }
         |${singleLine(serialize(doc))}""".stripMargin
    }
  }

  case class Update(config: ESConfig, doc: AnyRef, id: String) extends BulkAction {
    def jsonString: String = {
      s"""{ "update" : { "_index" : "${config.indexName}", "_type" : "${config.typeName.getOrElse("")}", "_id": "${id}"} } }
         |{ "doc": ${singleLine(serialize(doc))} }""".stripMargin
    }
  }

  case class Script(config: ESConfig, script: String, id: String) extends BulkAction {
    def jsonString: String = {
      s"""{ "update" : { "_index" : "${config.indexName}", "_type" : "${config.typeName.getOrElse("")}", "_id": "${id}"} } }
         |{ "script": ${script} }""".stripMargin
    }
  }

  case class Delete(config: ESConfig, id: String) extends BulkAction {
    def jsonString: String = {
      s"""{ "delete" : { "_index" : "${config.indexName}", "_type" : "${config.typeName.getOrElse("")}", "_id": "${id}"} } }"""
    }
  }

  private def singleLine(str: String): String = str.replace("\n", "").replace("\r", "")

}
