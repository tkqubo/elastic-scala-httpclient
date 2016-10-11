package jp.co.bizreach.elasticsearch4s
import org.scalatest.FunSuite

class ESConfigTest extends FunSuite {


  test("urlWithParameters 1") {
    assert(ESConfig("index", preference = Some("session"), explain = (true), timeout = Some(1000)).urlWithParameters("base","path") == "base/index/path?preference=session&explain=true&timeout=1000ms")
  }

  test("urlWithParameters 2") {
    assert(ESConfig("index", preference = None, explain = (true), timeout = Some(1000)).urlWithParameters("base","path") == "base/index/path?explain=true&timeout=1000ms")
  }

  test("urlWithParameters 3") {
    assert(ESConfig("index", preference = Some("preference"), explain = (false), timeout = Some(1000)).urlWithParameters("base","path") == "base/index/path?preference=preference&timeout=1000ms")
  }

  test("urlWithParameters 4") {
    assert(ESConfig("index", preference = Some("preference"), explain = (false), timeout = None).urlWithParameters("base","path") == "base/index/path?preference=preference")
  }

  test("urlWithParameters 5") {
    assert(ESConfig("index", preference = None, explain = (false), timeout = None).urlWithParameters("base","path") == "base/index/path")
  }

  test("urlWithParameters 6") {
    assert(ESConfig("index", preference = None, explain = (true), timeout = None).urlWithParameters("base","path") == "base/index/path?explain=true")
  }

}
