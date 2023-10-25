/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.testkit

import java.io.File

import org.bitlap.network.enumeration.{ OperationType, TypeId }
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.testkit.server.MockDriverIO

import org.scalatest.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

/** csv test
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
class CSVUtilsSpec extends AnyFunSuite with CSVUtils with should.Matchers {

  test("testCsvConvert1") {
    val csv = readClasspathCSVData("simple_data.csv")
    println(csv.headOption)
    val tmp = new File("./bitlap-testkit/target/simple_data.csv")
    val ret = writeCSVData(tmp, csv)
    assert(ret)
    tmp.delete()
    csv.headOption.toString shouldEqual "Some(Metric(1666195200,14,List(Dimension(os,Windows 8), Dimension(city,西安)),pv,712739626))"
  }

  test("testMockDriverIO") {
    val driverIO = new MockDriverIO()
    val ret      = driverIO.fetchResults(OperationHandle(OperationType.ExecuteStatement), 50, 1)
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
    value shouldEqual List(1666195200, 14, "Windows 8", "pv", 712739626)
  }
}
