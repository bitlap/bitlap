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
package org.bitlap.core.sql.parser.cmd

import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.BitlapEventWriter
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.hadoop.fs.Path

/** load data into table
 */
class SqlLoadData(
  override val _pos: SqlParserPos,
  private val filePath: SqlNode,
  private val tableName: SqlIdentifier,
  private val overwrite: Boolean)
    extends BitlapSqlDdlNode(_pos, SqlLoadData.OPERATOR, List(filePath, tableName)) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    val path = filePath.toString.stripPrefix("'").stripSuffix("'")
    val table = if (tableName.names.size == 1) {
      catalog.getTable(tableName.getSimple)
    } else {
      catalog.getTable(tableName.names.get(1), tableName.names.get(0))
    }
    // only support overwrite = true
    scala.util.Using.resource(BitlapEventWriter(table, BitlapContext.hadoopConf)) { it =>
      if (path.startsWith("classpath:") && path.length > 10) {
        val input = SqlLoadData.getClass.getClassLoader.getResourceAsStream(path.substring(10))
        if (input != null) {
          it.writeCsv(input)
        }
      }
      // hadoop file system should support protocal
      else {
        val p  = Path(path)
        val fs = p.getFileSystem(BitlapContext.hadoopConf)
        it.writeCsv(fs.open(p))
      }
    }
    List(Array(true))
  }
}

object SqlLoadData {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("LOAD DATA", SqlKind.OTHER)
}
