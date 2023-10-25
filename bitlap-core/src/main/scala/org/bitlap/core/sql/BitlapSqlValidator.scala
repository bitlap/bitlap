/**
 * Copyright (C) 2023 bitlap.org .
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

/** Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/9/10
 */
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
