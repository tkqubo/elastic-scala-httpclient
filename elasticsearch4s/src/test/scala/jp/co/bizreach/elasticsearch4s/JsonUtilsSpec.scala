package jp.co.bizreach.elasticsearch4s

import org.joda.time.DateTime
import org.scalatest._

class JsonUtilsSpec extends FunSuite {

  test("deserialize normally"){
    val sample1 = JsonUtils.deserialize[SimpleSample]("""{"name": "Naoki Takezoe"}""")
    assert(sample1.name === "Naoki Takezoe")

    // ignore unknown property
    val sample2 = JsonUtils.deserialize[SimpleSample]("""{"name": "Naoki Takezoe", "age": 35}""")
    assert(sample2.name === "Naoki Takezoe")
  }

  test("deserialize single value array"){
    // array to single property
    val sample1 = JsonUtils.deserialize[SimpleSample]("""{"name": ["Naoki Takezoe"]}""")
    assert(sample1.name === "Naoki Takezoe")

    // array to array property
    val sample2 = JsonUtils.deserialize[ArraySample]("""{"name": ["Naoki Takezoe"]}""")
    assert(sample2.name === Array("Naoki Takezoe"))
  }

  test("deserialize date property"){
    // array to single property
    val sample1 = JsonUtils.deserialize[DateSample]("""{"date": ["2015-02-25T21:10:12.456Z"]}""")
    assert(sample1.date.toString() === "2015-02-25T21:10:12.456Z")

    // array to array property
    val sample2 = JsonUtils.deserialize[DateArraySample]("""{"date": ["2015-02-25T21:10:12.456Z"]}""")
    assert(sample2.date.map(_.toString()) === Array("2015-02-25T21:10:12.456Z"))
  }
}

case class SimpleSample(name: String)

case class ArraySample(name: Array[String])

case class DateSample(date: DateTime)

case class DateArraySample(date: Array[DateTime])
