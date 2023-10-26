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
package org.bitlap.core.sql.table

import java.util.List as JList

import org.bitlap.core.catalog.metadata.Table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexNode

/** empty table
 */
class BitlapSqlQueryEmptyTable(override val table: Table) extends BitlapSqlQueryTable(table) {

  override def scan(root: DataContext, filters: JList[RexNode], projects: Array[Int]): Enumerable[Array[Any]] = {
    Linq4j.emptyEnumerable()
  }
}
