/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit
import com.github.javafaker.Faker

import java.util.concurrent.TimeUnit
import java.util.{ List => _, _ }
import java.text.SimpleDateFormat

/** bitlap 假数据创建工具
 *  @author
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
    val date = faker.date().past(7, TimeUnit.DAYS)
    val s    = sdf.format(date)
    sdf.parse(s).getTime / 1000
  }

  def randBigNumber: Long = faker.number().numberBetween(0, 1000000000)

  def randEntityNumber: Int = faker.number().numberBetween(0, 1000000)

  def randCity: String = faker.address().city()

  def randOs: String = os(faker.number().numberBetween(0, os.size))

  def randMetricName: String = metrics(faker.number().numberBetween(0, metrics.size))

  def randMetricValue: Long = faker.number().numberBetween(1, 10000)

  def randDimensions: List[Dimension] = {
    val dimensions = List(
      List(Dimension("city", randCity)),
      List(Dimension("os", randOs)),
      List(Dimension("os", randOs), Dimension("city", randCity))
    )
    dimensions(faker.number().numberBetween(0, dimensions.size))
  }

  def randMetrics(size: Long): List[Metric] =
    (0L until size).flatMap { _ =>
      val uid = randEntityNumber
      (0 until faker.number().numberBetween(0, 10)).map { _ =>
        Metric(
          randTimestamp,
          uid,
          randDimensions,
          randMetricName,
          // randBigNumber
          randMetricValue
        )
      }

    }.toList

}
