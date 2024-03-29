/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.testkit

import java.io.*

import bitlap.rolls.csv.*
import bitlap.rolls.csv.CSVUtils.*

/** CSV utils
 */
trait CSVUtils {

  given CSVFormat = new CSVFormat {
    override val hasHeaders: Boolean  = true
    override val hasColIndex: Boolean = false
  }

  def checkCSVData(file: File)(expect: List[Metric] => Boolean): Boolean =
    val (metadata, metrics) = CSVUtils.readCSV(
      FileName(file.getPath)
    ) { line =>
      line
        .into[Metric]
        .withFieldComputed(_.dimensions, dims => StringUtils.asClasses(dims)((k, v) => Dimension(k, v)))
        .decode
    }
    expect(metrics.toList)

  def readClasspathCSVData(file: String): List[Metric] =
    val (metadata, metrics) = CSVUtils.readCSV(
      FileName(this.getClass.getClassLoader.getResource(file).getFile)
    ) { line =>
      line
        .into[Metric]
        .withFieldComputed(_.dimensions, dims => StringUtils.asClasses(dims)((k, v) => Dimension(k, v)))
        .decode
    }
    metrics.toList

  def writeCSVData(file: File, metrics: List[Metric]): Boolean =
    if (file.exists()) file.delete() else file.createNewFile()
    val res = CSVUtils.writeCSV(file, metrics) { m =>
      m.into
        .withFieldComputed(_.dimensions, dims => StringUtils.asString(dims.map(f => f.key -> f.value).toList))
        .encode
    }
    res
}
