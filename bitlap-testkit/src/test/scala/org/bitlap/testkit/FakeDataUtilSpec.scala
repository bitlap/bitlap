/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import org.junit.Test
import java.io.*

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/24
 */
class FakeDataUtilSpec extends CSVUtils {

  @Test
  def testFakeData(): Unit = {
    val s: List[Metric] = FakeDataUtils.randMetrics(50)
    val file            = new File("./simple_data.csv")
    writeCsvData(file, s)

    val ms = readCsvData("./simple_data.csv")
    assert(s == ms)

    writeCsvData(file, ms)
    val newMs = readCsvData("./simple_data.csv")
    assert(newMs == s)

    file.delete()
  }

}
