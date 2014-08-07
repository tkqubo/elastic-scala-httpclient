elasticsearch4s
===============

Scala client and code generator for Elasticsearch

## How to use

Add a following dependency into your `build.sbt` at first.

```scala
libraryDependencies += "jp.co.bizreach" %% "elasticsearch4s" % "0.0.1"
```

You can access Elasticsearch via REST API as following:

```scala
case class Tweet(name: String, message: String)

import jp.co.bizreach.elasticsearch4s._

ESClient.using("http://localhost:9200"){ client =>
  val config = ESConfig("twitter", "tweet")
  
  // Insert
  client.insert(config, Tweet("takezoe", "Hello World!!"))
  client.insertJson(config, """{name: "takezoe", message: "Hello World!!"}""")
  
  // Update
  client.update(config, "1", Tweet("takezoe", "Hello Scala!!"))
  client.updateJson(config, "1", """{name: "takezoe", message: "Hello World!!"}""")
  
  // Delete
  client.delete(config, "1")
  
  // Find one document
  val tweet: Option[(String, Tweet)] = client.find[Tweet](config){ seacher =>
    seacher.setQuery(QueryBuilders.termQuery("_id", "1"))
  }
  
  // Search documents
  val list: List[ESSearchResult] = client.list[Tweet](config){ seacher =>
    seacher.setQuery(QueryBuilders.termQuery("name", "takezoe"))
  }
}
```

elasticsearch4s is a wrapper of Elasticsearch Java API. Therefore see [its document]( http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/) to know details, especially how to build query.

## Code Generator

TODO
