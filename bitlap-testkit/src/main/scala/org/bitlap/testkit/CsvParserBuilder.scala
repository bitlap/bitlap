/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import java.io.{ BufferedReader, FileReader, InputStreamReader }
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Using

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
trait CsvParserBuilder[T <: Product] {

  protected val list: ListBuffer[T] = ListBuffer.empty

  def fromInputStream[CPD <: Product](in: BufferedReader, complexDim: String)(implicit classTag: ClassTag[CPD]): List[T]

  def fromFile[CPD <: Product](fileName: String, complexDim: String)(implicit classTag: ClassTag[CPD]): List[T]

  def fromResourceFile[CPD <: Product](fileName: String, complexDim: String)(implicit classTag: ClassTag[CPD]): List[T]

}

object CsvParserBuilder {

  case class Dimension(key: String, value: String)

  case class Metric(time: Long, entity: Int, dimensions: Option[List[Dimension]] = None, name: String, value: Int)

  val defaultConverter: CsvConverter[Metric] = CsvConverter[Metric]

  val defaultParser: CsvParserBuilder[Metric] = new CsvParserBuilder[Metric] {
    private var curLine = 0
    override def fromInputStream[CPD <: Product](in: BufferedReader, complexDim: String)(implicit
      classTag: ClassTag[CPD]
    ): List[Metric] = {
      Using.resource(in) { input =>
        var line: String = null
        var idx = -1
        while ({
          line = input.readLine()
          line != null
        }) {
          curLine += 1
          val values = line.split(",").toSeq
          if (curLine == 1) {
            idx = values.indexOf(complexDim)
          } else {
            if (idx != -1) {
              val buffer = ListBuffer[String]()
              for (i <- values.indices)
                if (i != idx) {
                  buffer.append(values(i))
                }

              val metric = defaultConverter.from(buffer.result().mkString(","))
              val vs = values(idx).replace("{", "").replace("}", "").split(" ")
              val dimStrs = vs.map(kv => kv.trim.split(":")(0) -> kv.trim.split(":")(1))
              val dim = classTag.runtimeClass.getSimpleName match {
                case "Dimension" if idx != -1 =>
                  dimStrs.map(kv => Dimension(kv._1.trim.replace("\"", ""), kv._2.trim.replace("\"", ""))).toList
                case _ => Nil
              }
              metric.map(f => f.copy(dimensions = Some(dim))).foreach(f => list.append(f))

            } else {
              defaultConverter.from(line).foreach(f => list.append(f))
            }
          }
        }

      }
      list.result()
    }

    override def fromFile[CPD <: Product](fileName: String, complexDim: String)(implicit
      classTag: ClassTag[CPD]
    ): List[Metric] =
      fromInputStream[CPD](new BufferedReader(new FileReader(fileName)), complexDim)

    override def fromResourceFile[CPD <: Product](fileName: String, complexDim: String)(implicit
      classTag: ClassTag[CPD]
    ): List[Metric] = {
      val input = ClassLoader.getSystemResourceAsStream(fileName)
      val reader = new InputStreamReader(input)
      val bufferedReader = new BufferedReader(reader)
      fromInputStream[CPD](bufferedReader, complexDim)
    }
  }

}
