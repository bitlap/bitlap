/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli.interactive

import sqlline.SqlLineProperty
import sqlline.SqlLineProperty.Type

import java.util
import scala.jdk.CollectionConverters._

class BitlapSqlLineProperty(
  name: String,
  typ: Type,
  defaultValue: AnyRef,
  couldBeStored: Boolean = true,
  isReadOnly: Boolean = false,
  availableValues: Set[String] = Set.empty
) extends SqlLineProperty {

  override def propertyName(): String = name

  override def defaultValue(): AnyRef = defaultValue

  override def isReadOnly: Boolean = isReadOnly

  override def couldBeStored: Boolean = couldBeStored

  override def `type`(): Type = typ

  override def getAvailableValues: util.Set[String] = availableValues.asJava
}

case object BitlapSqlLineDefaultProperty extends BitlapSqlLineProperty("bitlapPromp", Type.STRING, "bitlap")
