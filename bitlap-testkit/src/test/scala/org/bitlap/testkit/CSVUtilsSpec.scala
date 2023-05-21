/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import java.io.File

import org.bitlap.network.enumeration.{ OperationType, TypeId }
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.testkit.server.MockAsyncRpcBackend

import org.junit.Assert.assertEquals
import org.junit.Test

/** csv test
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
class CSVUtilsSpec extends CSVUtils {

  @Test
  def testCsvConvert1(): Unit = {
    val csv = readClasspathCSVData("simple_data.csv")
    println(csv.headOption)
    assertEquals(
      "Some(Metric(1666195200,14,List(Dimension(os,Windows 8), Dimension(city,西安)),pv,712739626))",
      csv.headOption.toString
    )

    val tmp = new File("./simple_data.csv")
    val ret = writeCSVData(tmp, csv)
    assert(ret)
    tmp.delete()
  }

  @Test
  def testMockZioRpcBackend1(): Unit = {
    val backend = new MockAsyncRpcBackend()
    val ret     = backend.fetchResults(new OperationHandle(OperationType.ExecuteStatement), 50, 1)
    val syncRet = zio.Unsafe.unsafe { implicit rt =>
      zio.Runtime.default.unsafe.run(ret).getOrThrowFiberFailure()
    }
    println(syncRet)
    val head = syncRet.results.rows.head.values
    val value = List(
      BitlapSerde.deserialize[Long](TypeId.LongType, head(0)),
      BitlapSerde.deserialize[Int](TypeId.IntType, head(1)),
      BitlapSerde.deserialize[String](TypeId.StringType, head(2)),
      BitlapSerde.deserialize[String](TypeId.StringType, head(3)),
      BitlapSerde.deserialize[Long](TypeId.LongType, head(4))
    )
    assertEquals(
      value,
      List(1666195200, 14, "Windows 8", "pv", 712739626)
    )
  }
}
