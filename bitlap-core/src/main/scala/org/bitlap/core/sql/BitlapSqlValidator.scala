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
package org.bitlap.core.sql

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorImpl

class BitlapSqlValidator(
  val _opTab: SqlOperatorTable,
  val _catalogReader: CalciteCatalogReader,
  val _config: SqlValidator.Config,
  val connConfig: CalciteConnectionConfig)
    extends SqlValidatorImpl(_opTab, _catalogReader, _catalogReader.getTypeFactory, _config) {

  private val queryContext = QueryContext.get()

  override def validate(topNode: SqlNode): SqlNode = {
    super.validate(topNode)
  }

  override def validateSelect(select: SqlSelect, targetRowType: RelDataType): Unit = {
    queryContext.currentSelectNode = select
    super.validateSelect(select, targetRowType)
  }
}
