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
package org.bitlap.network.enumeration

import org.bitlap.network.Driver.*

import enumeratum.values.*

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

  def toGetInfoType(getInfoType: BGetInfoType): GetInfoType =
    GetInfoType
      .withValueOpt(getInfoType.value)
      .getOrElse(throw new IllegalArgumentException(s"Invalid GetInfoType: $getInfoType"))

  def toBGetInfoType(getInfoType: GetInfoType): BGetInfoType =
    BGetInfoType.fromValue(getInfoType.value)
}
