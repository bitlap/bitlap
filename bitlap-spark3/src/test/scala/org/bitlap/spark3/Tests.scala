package org.bitlap.spark3

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * Desc:
 * 
 * Mail: k.chen@nio.com
 * Created by IceMimosa
 * Date: 2023/4/14
 */
object Tests {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*,2]")
      .getOrCreate()
    import spark.implicits._

    val df = Seq(("John", 31), ("Bob", 28)).toDF("name", "age")

    df.show(false)

    df.write
      .format("bitlap")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
