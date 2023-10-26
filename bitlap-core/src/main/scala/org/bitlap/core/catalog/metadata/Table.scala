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
package org.bitlap.core.catalog.metadata

import scala.collection.mutable

/** Table metadata
 */
final case class Table(
  database: String,
  name: String,
  createTime: Long = System.currentTimeMillis(),
  var updateTime: Long = System.currentTimeMillis(),
  props: mutable.Map[String, String] = mutable.Map(),
  // other fields
  path: String) {

  import Table._

  override def toString: String = s"$database.$name"

  def getTableFormat: String = this.props(TABLE_FORMAT_KEY)
}

object Table {
  // table properties
  val TABLE_FORMAT_KEY = "table_format"
}
