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
package org.bitlap.core.hadoop

import scala.reflect.ClassTag
import scala.util.Using

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.utils.JsonUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, FileSystem, LocatedFileStatus, Path }
import org.apache.hadoop.hdfs.protocol.HdfsNamedFileStatus

extension (fs: FileSystem) {

  /** Write bitlap table schema.
   */
  def writeTable[A](tableDir: Path, table: Table)(effect: => A): Boolean = {
    Using.resource(fs.create(Path(tableDir, ".table"), true)) { o =>
      o.writeUTF(JsonUtil.json(table))
    }
    effect
    true
  }

  /** Read bitlap table schema.
   */
  def readTable(tableDir: Path): Table = {
    Using.resource(fs.open(Path(tableDir, ".table"))) { in =>
      JsonUtil.jsonAs(in.readUTF(), classOf[Table])
    }
  }

  /** clone conf
   */
  def newConf(): Configuration = {
    Configuration(fs.getConf)
  }
}

extension (fs: FileSystem)

  def collectStatus[A: ClassTag](dbDir: Path, a: FileStatus => Boolean)(effect: (FileSystem, FileStatus) => A)
    : List[A] = {
    fs.listStatus(dbDir)
      .collect {
        case status: FileStatus if a(status) => effect.apply(fs, status)
      }
      .toList
  }
