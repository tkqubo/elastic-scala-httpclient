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

import jp.co.bizreach.elasticsearch4s.ESClient
import jp.co.bizreach.elasticsearch4s.ESClient._

ESClient.using("http://localhost:9200"){ client =>
  implicit val config = ESConfig("twitter", "tweet")
  
  // Insert
  client.insert(Tweet("takezoe", "Hello World!!"))
  client.insertJson("""{name: "takezoe", message: "Hello World!!"}""")
  
  // Update
  client.update("1", Tweet("takezoe", "Hello Scala!!"))
  client.updateJson("1", """{name: "takezoe", message: "Hello World!!"}""")
  
  // Delete
  client.delete("1")
  
  // Find one document
  val tweet: Option[(String, Tweet)] = client.find(classOf[Tweet]){ seacher =>
    seacher.setQuery(QueryBuilders.termQuery("name", "takezoe"))
  }
  
  // Search documents
  val list: List[ESSearchResult] = client.list(classOf[Tweet]){ seacher =>
    seacher.setQuery(QueryBuilders.termQuery("name", "takezoe"))
  }
}
```

## Code Generator

TODO
