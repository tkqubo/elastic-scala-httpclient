package jp.co.bizreach.elasticsearch4s.generator

import java.io.{FileInputStream, File}
import org.apache.commons.io.{FileUtils, IOUtils}

object Utils {

  def read(file: File): String = IOUtils.toString(new FileInputStream(file), "UTF-8")

  def write(file: File, content: String): Unit = FileUtils.write(file, content, "UTF-8")

  def toUpperCamel(name: String) = name.substring(0, 1).toUpperCase + name.substring(1)

  def toLowerCamel(name: String) = name.substring(0, 1).toLowerCase + name.substring(1)


}
