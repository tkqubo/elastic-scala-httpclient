package jp.co.bizreach.elasticsearch4s

import org.scalatest._
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

class JsonUtilsSpec extends FunSuite {

  test("deserialize normally"){
    val sample1 = JsonUtils.deserialize[Sample]("""{"name": "Naoki Takezoe"}""")
    assert(sample1.name === "Naoki Takezoe")

    val sample2 = JsonUtils.deserialize[Sample]("""{"name": "Naoki Takezoe", "age": 35}""")
    assert(sample2.name === "Naoki Takezoe")
  }

}

@JsonIgnoreProperties(ignoreUnknown = true)
case class Sample(name: String)
