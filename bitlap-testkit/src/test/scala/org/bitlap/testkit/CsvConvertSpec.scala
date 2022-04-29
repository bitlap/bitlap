/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import junit.framework.TestCase
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.OperationType
import org.bitlap.testkit.csv.{ CsvConverter, CsvParserBuilder, CsvParserSetting }
import org.bitlap.testkit.server.MockZioRpcBackend
import org.junit.Assert.{ assertEquals, assertTrue }
import org.junit.Test

import scala.reflect.classTag
import scala.util.Success

/**
 * csv test
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
class CsvConvertSpec extends TestCase("CsvConvertSpec") {

  @Test
  def testCsvConvert1 {
    val input = """John,Carmack,23,0,,100
               Brian,Fargo,35,,,110
               Markus,Persson,32,,,120"""

    val persons = CsvConverter[List[Person]].from(input)
    println(persons)
    assertTrue(persons.getOrElse(List.empty).size == 3)
    assertEquals(persons.map(_.head), Success(Person("John", "Carmack", 23, Some(0), None, 100)))
  }

  @Test
  def testCsvLoaderBuilder1 {
    val input = CsvParserSetting
      .builder[Dimension]()
      .fileName("simple_data.csv")
      .classTag(classTag = classTag[Dimension])
      .dimensionName("dimensions")
      .build()

    println(input)
    val metrics = CsvParserBuilder.MetricParser.fromResourceFile[Dimension](input)
    println(metrics)
    assertEquals(metrics.size, 16)
    assertEquals(metrics.head.dimensions, Some(List(Dimension("city", "北京"), Dimension("os", "Mac"))))
  }

  @Test
  def testMockZioRpcBackend1 {
    val backend = MockZioRpcBackend()
    val ret = backend.fetchResults(new OperationHandle(OperationType.EXECUTE_STATEMENT))
    val syncRet = zio.Runtime.default.unsafeRun(ret)
    println(syncRet)
    assertEquals(syncRet.results.rows.head.values.map(v => v.toStringUtf8), List("100", "1", "北京", "vv", "1"))
  }
}
