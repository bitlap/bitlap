/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark3

import org.bitlap.spark.*
import org.apache.spark.sql.SparkSession
import zio.ZIO

/** @since 2022/10/14
 *  @author
 *    梦境迷离
 */
object SparkSpec extends App {

  val url = "jdbc:bitlap://localhost:23333/default"

  val session = SparkSession
    .builder()
    .appName("bitlap")
    .master("local[*,2]") // config
    .getOrCreate()

  val ret = for {
    df <- ZIO.attempt(
      session.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .option("quote", "\"")
        .option("escape", "\"")
        .load("./bitlap-testkit/src/main/resources/simple_data.csv")
    )
    _ <- ZIO.attempt(df.show())
    _ <- df.liftDataFrameWriter.map(_.mode("append").options(Map("url" -> url, "table" -> "tb_dimension")).save())
    _ <- session.liftDataFrameReader.map(
      _.options(
        Map(
          "url"   -> url,
          "table" -> "tb_dimension",
          "sql"   -> "select _time,1,'str','str', sum(pv) as pv from tb_dimension where _time >= 100 group by _time"
        ) // 这个SQL必须是五列和read时的schema相同，因为schema是写死的，用TestEmbedBitlapServer测试
      ).load().show()
    )
  } yield {}

  zio.Unsafe.unsafe { implicit rt =>
    zio.Runtime.default.unsafe.run(ret).getOrThrowFiberFailure()
  }

}
