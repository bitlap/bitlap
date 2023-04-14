/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import org.bitlap.network.enumeration.OperationType
import org.bitlap.network.handles.OperationHandle
import org.bitlap.testkit.server.MockAsyncRpcBackend
import org.junit.Assert.assertEquals
import org.junit.Test

import java.io.File

/** csv test
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
class CsvConvertSpec extends CSVUtils {

  @Test
  def testCsvConvert1 {
    val csv = readCsvData("simple_data.csv")
    println(csv.headOption)
    assertEquals(
      "Some(Metric(1666195200,14,List(Dimension(os,Windows 8), Dimension(city,西安)),pv,712739626))",
      csv.headOption.toString
    )

    val tmp = new File("simple_data.csv")
    val ret = writeCsvData(tmp, csv)
    assert(ret)
    tmp.delete()
  }

  @Test
  def testMockZioRpcBackend1 {
    val backend = new MockAsyncRpcBackend()
    val ret     = backend.fetchResults(new OperationHandle(OperationType.ExecuteStatement), 50, 1)
    val syncRet = zio.Unsafe.unsafe { implicit rt =>
      zio.Runtime.default.unsafe.run(ret).getOrThrowFiberFailure()
    }
    println(syncRet)
    assertEquals(
      syncRet.results.rows.head.values.map(v => v.toStringUtf8),
      List("1666195200", "14", "Windows 8", "pv", "712739626")
    )
  }
}
