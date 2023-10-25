/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.testkit

import java.io.*

import org.scalatest.funsuite.AnyFunSuite

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/24
 */
class FakeDataUtilSpec extends AnyFunSuite with CSVUtils {

  test("testFakeData") {
    val s: List[Metric] = FakeDataUtils.randMetrics(50)
    val file            = new File("simple_data.csv")
    writeCSVData(file, s)
    assert(checkCSVData(file)(ms => ms == s))
    file.delete()
  }

}
