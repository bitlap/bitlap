/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import java.io.*

import org.junit.Test

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/24
 */
class FakeDataUtilSpec extends CSVUtils {

  @Test
  def testFakeData(): Unit = {
    val s: List[Metric] = FakeDataUtils.randMetrics(50)
    val file            = new File("./simple_data.csv")
    writeCSVData(file, s)

    val ms = readCSVData(new File("./simple_data.csv"))
    assert(s == ms)

    writeCSVData(file, ms)
    val newMs = readCSVData(new File("./simple_data.csv"))
    assert(newMs == s)

    file.delete()
  }

}
