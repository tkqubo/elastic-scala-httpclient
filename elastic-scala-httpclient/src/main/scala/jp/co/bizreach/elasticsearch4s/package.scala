package jp.co.bizreach

package object elasticsearch4s {

  def string2config(indexName: String) = ESConfig(indexName)
  def tuple2config(tuple: (String, String)) = ESConfig(tuple._1, tuple._2)

  implicit class ESStringConfig(indexName: String){
    def / (typeName: String): ESConfig = ESConfig(indexName, typeName)
  }

}
