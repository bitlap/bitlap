/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core

import scala.util.Using

import org.bitlap.common.utils.JsonEx
import org.bitlap.core.catalog.metadata.Table

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

package object hadoop {

  extension (fs: FileSystem) {

    /** Write bitlap table schema.
     */
    def writeTable(tableDir: Path, table: Table): Boolean = {
      Using.resource(fs.create(Path(tableDir, ".table"), true)) { o =>
        o.writeUTF(JsonEx.json(table))
      }
      return true
    }

    /** Read bitlap table schema.
     */
    def readTable(tableDir: Path): Table = {
      return Using.resource(fs.open(Path(tableDir, ".table"))) { in =>
        JsonEx.jsonAs(in.readUTF(), classOf[Table])
      }
    }

    /** clone conf
     */
    def newConf(): Configuration = {
      return Configuration(fs.getConf)
    }
  }
}
