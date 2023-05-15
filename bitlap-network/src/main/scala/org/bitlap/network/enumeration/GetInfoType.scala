/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.enumeration

import enumeratum.values._
import org.bitlap.network.driver_proto._

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/18
 */
sealed abstract class GetInfoType(val value: Int) extends IntEnumEntry
object GetInfoType extends IntEnum[GetInfoType] {
  final case object MaxDriverConnections    extends GetInfoType(1)
  final case object MaxConcurrentActivities extends GetInfoType(10)
  final case object DataSourceName          extends GetInfoType(20)
  final case object ServerName              extends GetInfoType(30)
  final case object ServerConf              extends GetInfoType(31)
  final case object DbmsName                extends GetInfoType(40)
  final case object DbmsVer                 extends GetInfoType(50)

  val values: IndexedSeq[GetInfoType] = findValues

  def toGetInfoType(bGetInfoType: BGetInfoType): GetInfoType =
    GetInfoType.withValueOpt(bGetInfoType.value).getOrElse(throw new Exception("Invalid GetInfoType"))

  def toBGetInfoType(getInfoType: GetInfoType): BGetInfoType =
    BGetInfoType.fromValue(getInfoType.value)

}
