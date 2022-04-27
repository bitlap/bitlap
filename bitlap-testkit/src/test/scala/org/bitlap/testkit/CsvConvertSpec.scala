/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.testkit.CsvParserBuilder.Dimension
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

/**
 * csv test
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
class CsvConvertSpec extends AnyFlatSpec with Matchers {

  "CsvConvert1" should "ok for case class" in {
    import CsvConverter._

    val input = """John,Carmack,23,0,,100
               Brian,Fargo,35,,,110
               Markus,Persson,32,,,120"""

    val persons = CsvConverter[List[Person]].from(input)
    println(persons)
    persons.getOrElse(List.empty).size shouldBe 3
    persons.map(_.head) shouldEqual Success(Person("John", "Carmack", 23, Some(0), None, 100))
  }

  "CsvLoaderBuilder1" should "ok for Metric" in {
    val metrics = CsvParserBuilder.defaultParser.fromResourceFile[Dimension]("simple_data.csv", "dimensions")
    println(metrics)
    metrics.size shouldBe 16
    metrics.head.dimensions shouldBe  Some(List(Dimension("city", "北京"),Dimension("os", "Mac")))
  }
}
