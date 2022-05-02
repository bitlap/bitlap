/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import junit.framework.TestCase
import org.bitlap.network.OperationType
import org.bitlap.network.handles.OperationHandle
import org.bitlap.testkit.server.MockZioRpcBackend
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * csv test
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
class CsvConvertSpec extends TestCase("CsvConvertSpec") with CsvHelper {

  @Test
  def testCsvConvert1 {
    val csv = readCsvData("simple_data.csv")
    println(csv)
    assertTrue(csv.toString() == "List(Metric(100,1,List(Dimension(city,北京), Dimension(os,Mac)),vv,1))")
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
