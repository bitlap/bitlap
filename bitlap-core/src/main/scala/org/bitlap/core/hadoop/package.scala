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
package org.bitlap.core

import scala.util.Using

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.utils.JsonUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

package object hadoop {

  extension (fs: FileSystem) {

    /** Write bitlap table schema.
     */
    def writeTable(tableDir: Path, table: Table): Boolean = {
      Using.resource(fs.create(Path(tableDir, ".table"), true)) { o =>
        o.writeUTF(JsonUtil.json(table))
      }
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
}
