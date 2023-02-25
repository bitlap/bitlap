/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import java.io.File
import org.junit.Test
import java.io.FileInputStream

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/24
 */
class FakeDataUtilSpec extends CsvUtil {

  @Test
  def testFakeData(): Unit = {
    val s: List[Metric] = FakeDataUtil.randMetrics(50)
    val file            = new File("./simple_data.csv")
    writeCsvData(file, s)

    val fileInputStream1 = new FileInputStream(new File("./simple_data.csv"))
    val ms               = readCsvData(fileInputStream1)
    assert(s == ms)

    writeCsvData(file, ms)
    val fileInputStream2 = new FileInputStream(new File("./simple_data.csv"))
    val newMs            = readCsvData(fileInputStream2)
    assert(newMs == s)

    file.delete()
  }

}
