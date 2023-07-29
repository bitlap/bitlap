/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network.enumeration

import org.bitlap.network.Driver.*

import enumeratum.values.*

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/18
 */
sealed abstract class GetInfoType(val value: Int) extends IntEnumEntry

object GetInfoType extends IntEnum[GetInfoType] {
  case object MaxDriverConnections extends GetInfoType(1)

  case object MaxConcurrentActivities extends GetInfoType(10)

  case object DataSourceName extends GetInfoType(20)

  case object ServerName extends GetInfoType(30)

  case object ServerConf extends GetInfoType(31)

  case object DbmsName extends GetInfoType(40)

  case object DbmsVer extends GetInfoType(50)

  val values: IndexedSeq[GetInfoType] = findValues

  def toGetInfoType(bGetInfoType: BGetInfoType): GetInfoType =
    GetInfoType.withValueOpt(bGetInfoType.value).getOrElse(throw new Exception("Invalid GetInfoType"))

  def toBGetInfoType(getInfoType: GetInfoType): BGetInfoType =
    BGetInfoType.fromValue(getInfoType.value)
}
