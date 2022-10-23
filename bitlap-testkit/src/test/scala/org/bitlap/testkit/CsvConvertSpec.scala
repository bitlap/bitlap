/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import junit.framework.TestCase
import org.bitlap.network.OperationType
import org.bitlap.network.handles.OperationHandle
import org.bitlap.testkit.server.MockZioRpcBackend
import org.junit.Assert.assertEquals
import org.junit.Test

/** csv test
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
class CsvConvertSpec extends TestCase("CsvConvertSpec") with CsvUtil {

  @Test
  def testCsvConvert1 {
    val csv = readCsvData("simple_data.csv")
    println(csv)
    assertEquals(
      "List(Metric(100,1,List(Dimension(city,北京), Dimension(os,Mac)),vv,1), Metric(100,1,List(Dimension(city,北京), Dimension(os,Mac)),pv,2), Metric(100,1,List(Dimension(city,北京), Dimension(os,Windows)),vv,1), Metric(100,1,List(Dimension(city,北京), Dimension(os,Windows)),pv,3), Metric(100,2,List(Dimension(city,北京), Dimension(os,Mac)),vv,1), Metric(100,2,List(Dimension(city,北京), Dimension(os,Mac)),pv,5), Metric(100,3,List(Dimension(city,北京), Dimension(os,Mac)),vv,1), Metric(100,3,List(Dimension(city,北京), Dimension(os,Mac)),pv,2), Metric(200,1,List(Dimension(city,北京), Dimension(os,Mac)),vv,1), Metric(200,1,List(Dimension(city,北京), Dimension(os,Mac)),pv,2), Metric(200,1,List(Dimension(city,北京), Dimension(os,Windows)),vv,1), Metric(200,1,List(Dimension(city,北京), Dimension(os,Windows)),pv,3), Metric(200,2,List(Dimension(city,北京), Dimension(os,Mac)),vv,1), Metric(200,2,List(Dimension(city,北京), Dimension(os,Mac)),pv,5), Metric(200,3,List(Dimension(city,北京), Dimension(os,Mac)),vv,1), Metric(200,3,List(Dimension(city,北京), Dimension(os,Mac)),pv,2))",
      csv.toString()
    )
  }

  @Test
  def testMockZioRpcBackend1 {
    val backend = MockZioRpcBackend()
    val ret     = backend.fetchResults(new OperationHandle(OperationType.ExecuteStatement), 50, 1)
    val syncRet = zio.Runtime.default.unsafeRun(ret)
    println(syncRet)
    assertEquals(syncRet.results.rows.head.values.map(v => v.toStringUtf8), List("100", "1", "北京", "vv", "1"))
  }
}
