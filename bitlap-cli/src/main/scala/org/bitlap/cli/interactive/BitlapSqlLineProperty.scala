/**
 * Copyright (C) 2023 bitlap.org .
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
