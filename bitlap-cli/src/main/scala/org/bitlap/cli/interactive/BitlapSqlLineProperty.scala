/* Copyright (c) 2023 bitlap.org */
package org.bitlap.cli.interactive

import sqlline.SqlLineProperty
import sqlline.SqlLineProperty.Type

import java.util.{ Set => JSet }
import scala.jdk.CollectionConverters._

class BitlapSqlLineProperty(
  name: String,
  `type`: Type,
  defaultValue: AnyRef,
  couldBeStored: Boolean = true,
  isReadOnly: Boolean = false,
  availableValues: Set[String] = Set.empty
) extends SqlLineProperty {

  override def propertyName(): String = name

  override def defaultValue(): AnyRef = defaultValue

  override def isReadOnly: Boolean = isReadOnly

  override def couldBeStored: Boolean = couldBeStored

  override def `type`(): Type = `type`

  override def getAvailableValues: JSet[String] = availableValues.asJava
}

object BitlapSqlLineProperty {
  // sql line prompt, default is: $ bitlap>
  case object BitlapPrompt extends BitlapSqlLineProperty("bitlapPrompt", Type.STRING, "bitlap")
}
