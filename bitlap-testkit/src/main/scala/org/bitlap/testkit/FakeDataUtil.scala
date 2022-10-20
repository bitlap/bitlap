/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit
import com.github.javafaker.Faker

import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.Random
import java.text.SimpleDateFormat

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/20
 */
object FakeDataUtil {

  private lazy val random: Random = new Random(System.nanoTime())

  private lazy val faker: Faker = new Faker(new Locale("zh-CN"), random)

  // get from javafaker's src/main/resources/en/device.yml
  private lazy val os: Seq[String] = Seq(
    "Android OS",
    "webOS",
    "iOS",
    "BlackBerry",
    "Danger OS",
    "Android",
    "Firefox OS",
    "Ubuntu Touch",
    "Windows Phone",
    "Windows 8",
    "Windows RT",
    "Windows 8.1",
    "Windows 10",
    "Windows 10 Mobile",
    "Windows Phone"
  )

  private lazy val metrics = Seq(
    "pv",
    "vv"
  )

  private final val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def randTimestamp: Long = {
    val date = faker.date().past(10, TimeUnit.DAYS)
    val s    = sdf.format(date)
    sdf.parse(s).getTime / 1000
  }

  def randBigNumber: Long = faker.number().numberBetween(0, 1000000000)

  def randSmallNumber: Int = faker.number().numberBetween(0, 100)

  def randCity: String = faker.address().city()

  def randOs: String = os(faker.number().numberBetween(0, os.size))

  def randMetricName: String = metrics(faker.number().numberBetween(0, metrics.size))

  def randDimensions: List[Dimension] = {
    val dimensions = List(
      List(Dimension("city", randCity)),
      List(Dimension("os", randOs)),
      List(Dimension("os", randOs), Dimension("city", randCity))
    )
    dimensions(faker.number().numberBetween(0, dimensions.size))
  }

  def randMetrics(size: Long): List[Metric] =
    (0L until size).map { _ =>
      Metric(
        randTimestamp,
        randSmallNumber,
        randDimensions,
        randMetricName,
        randBigNumber
      )

    }.toList

}
