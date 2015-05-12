elastic-scala-httpclient   [![Build Status](https://secure.travis-ci.org/bizreach/elasticsearch4s.png?branch=master)](http://travis-ci.org/bizreach/elasticsearch4s)
===============

Elasticsearch HTTP client for Scala with code generator.

## How to use

Add a following dependency into your `build.sbt` at first.

```scala
libraryDependencies += "jp.co.bizreach" %% "elastic-scala-httpclient" % "1.0.0"
```

You can access Elasticsearch via HTTP Rest API as following:

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

elastic-scala-gen can generate source code from Elasticsearch schema json file.

At first, add following setting into `project/plugins.sbt`:

```scala
addSbtPlugin("jp.co.bizreach" % "elastic-scala-gen" % "1.0.0")
```

Then put Elasticsearch schema json file as `PROJECT_ROOT/schema.json` and execute `sbt es-gen`. Source code will be generated into `src/main/scala/models`.

You can configure generation settings in `PROJECT_ROOT/es-gen.conf`. Here is a configuration example:

```properties
# package name
es-gen.package.name=models
# schema definition files
es-gen.json.files=[
  "./schema/book_master.json",
  "./schema/book_search.json"
]
# map same index name to different classes
es-gen.class.mappings=[
  "book_master.json#book:BookMaster",
  "book_search.json#book:BookSearch"
]
# map unknown type
es-gen.type.mappings=[
  "minhash:String"
]
# specify array properties
es-gen.array.properties=[
  "BookMaster.author",
  "BookSearch.author"
]
# specify ignore properties
es-gen.ignore.properties=[
  "internalCode"
]
```

See [ESCodegenConfig.scala](https://github.com/bizreach/elastic-scala-httpclient/blob/master/elastic-scala-gen/src/main/scala/jp/co/bizreach/elasticsearch4s/generator/ESCodegenConfig.scala) to know configuration details.
