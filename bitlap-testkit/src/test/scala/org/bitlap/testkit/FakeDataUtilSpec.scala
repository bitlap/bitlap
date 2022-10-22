/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import java.io.File

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/20
 */
object FakeDataUtilSpec extends CsvUtil with App {

  def generateFakeData(): Boolean = {
    val s: List[Metric] = FakeDataUtil.randMetrics(50)
    val file            = new File("./bitlap-testkit/src/main/resources/simple_data.csv")
    writeCsvData(file, s)
  }

  generateFakeData()
}
