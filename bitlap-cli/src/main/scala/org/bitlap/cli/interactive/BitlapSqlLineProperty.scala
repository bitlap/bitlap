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
package org.bitlap.cli.interactive

import java.util.Set as JSet

import scala.jdk.CollectionConverters.*

import sqlline.SqlLineProperty
import sqlline.SqlLineProperty.Type

class BitlapSqlLineProperty(
  name: String,
  _type: Type,
  _defaultValue: AnyRef,
  _couldBeStored: Boolean = true,
  _isReadOnly: Boolean = false,
  _availableValues: Set[String] = Set.empty)
    extends SqlLineProperty {

  override def propertyName(): String = name

  override def defaultValue(): AnyRef = _defaultValue

  override def isReadOnly: Boolean = _isReadOnly

  override def couldBeStored: Boolean = _couldBeStored

  override def `type`(): Type = _type

  override def getAvailableValues: JSet[String] = _availableValues.asJava
}

object BitlapSqlLineProperty {
  // sql line prompt, default is: $ bitlap>
  case object BitlapPrompt extends BitlapSqlLineProperty("bitlapPrompt", Type.STRING, "bitlap")
}
