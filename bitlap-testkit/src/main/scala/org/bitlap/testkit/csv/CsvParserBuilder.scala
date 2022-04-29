/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.csv

import org.bitlap.testkit.{ Dimension, Metric }

import java.io.{ BufferedReader, FileReader, InputStreamReader }
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Using

/**
 * Only support single dimension column.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
trait CsvParserBuilder[T <: Product] {

  protected val list: ListBuffer[T] = ListBuffer.empty

  def fromFile[Dim <: Product](inputBuilder: CsvParserSetting[Dim]): List[T]

  def fromResourceFile[Dim <: Product](inputBuilder: CsvParserSetting[Dim]): List[T]

}

object CsvParserBuilder {

  trait CsvLineHandle[T, D] {
    def converter: CsvConverter[T]
    def appendResult(values: Seq[String], idx: Int, classTag: ClassTag[D]): ListBuffer[String] => Unit
    def simpleAppendResult: String => Unit
  }

  private def parseCsvLine[T, D](idx: Int, handle: CsvLineHandle[T, D], line: String, complexDim: String)(implicit
    classTag: ClassTag[D]
  ): Unit = {
    val values = line.split(",").toSeq
    if (idx != -1) {
      val buffer = ListBuffer[String]()
      for (i <- values.indices)
        if (i != idx) {
          buffer.append(values(i))
        } else {
          buffer.append("")
        }
      handle.appendResult(values, idx, classTag).apply(buffer)
    } else {
      handle.simpleAppendResult(line)
    }
  }

  val MetricConverter: CsvConverter[Metric] = CsvConverter[Metric]

  val MetricParser: CsvParserBuilder[Metric] = new CsvParserBuilder[Metric] {
    private var curLine = 0
    private def fromInputStream[Dim <: Product](in: BufferedReader, complexDim: String)(implicit
      classTag: ClassTag[Dim]
    ): List[Metric] = {
      Using.resource(in) { input =>
        var line: String = null
        var idx: Int = -1
        while ({
          line = input.readLine()
          line != null
        }) {
          curLine += 1
          if (curLine == 1) {
            val values = line.split(",").toSeq
            idx = values.indexOf(complexDim)
          } else {
            parseCsvLine(idx, metricCsvLineHandle, line, complexDim)
          }
        }

      }
      list.result()
    }

    private val metricCsvLineHandle = new CsvLineHandle[Metric, Dimension] {
      override def converter: CsvConverter[Metric] = MetricConverter
      override def appendResult(
        values: Seq[String],
        idx: Int,
        classTag: ClassTag[Dimension]
      ): ListBuffer[String] => Unit = buffer => {
        val metric = converter.from(buffer.result().mkString(","))
        val vs = values(idx).replace("{", "").replace("}", "").split(" ")
        val dimStrs = vs.map(kv => kv.trim.split(":")(0) -> kv.trim.split(":")(1))
        val dim = classTag.runtimeClass.getSimpleName match {
          case "Dimension" if idx != -1 =>
            dimStrs.map(kv => Dimension(kv._1.trim.replace("\"", ""), kv._2.trim.replace("\"", ""))).toList
          case _ => Nil
        }
        metric.map(f => f.copy(dimensions = Some(dim))).foreach(f => list.append(f))
      }

      override def simpleAppendResult: String => Unit = line => {
        converter.from(line).foreach(f => list.append(f))
      }
    }

    override def fromFile[Dim <: Product](inputBuilder: CsvParserSetting[Dim]): List[Metric] =
      fromInputStream[Dim](new BufferedReader(new FileReader(inputBuilder.fileName)), inputBuilder.dimensionName)(
        inputBuilder.classTag
      )

    override def fromResourceFile[Dim <: Product](inputBuilder: CsvParserSetting[Dim]): List[Metric] = {
      val input = ClassLoader.getSystemResourceAsStream(inputBuilder.fileName)
      val reader = new InputStreamReader(input)
      val bufferedReader = new BufferedReader(reader)
      fromInputStream[Dim](bufferedReader, inputBuilder.dimensionName)(inputBuilder.classTag)
    }
  }

}
