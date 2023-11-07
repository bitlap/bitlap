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
package org.bitlap.common.conf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.{ classTag, ClassTag }

import org.bitlap.common.BitlapConf
import org.bitlap.common.extension.*

/** Key for [[org.bitlap.common.BitlapConf]]
 */
class BitlapConfKey[T: ClassTag](val key: String, val defaultValue: T = null) {

  val `type`: Class[_] = classTag[T].runtimeClass

  {
    BitlapConfKey.cache.put(key, this)
  }

  /** Assign default value by function
   */
  var defaultBy: BitlapConf => T = { _ => this.defaultValue }

  /** System property, default is: bitlap.[key]
   */
  var sys = ""

  def getSysKey: String = {
    if (this.sys != null && !this.sys.isBlank) {
      return this.sys
    }
    key
  }

  /** System environment property, default is: BITLAP_[key] (uppercase)
   */
  var env = ""

  def getEnvKey: String = {
    if (this.env != null && !this.env.isBlank) {
      return this.env
    }
    key.replace(".", "_").toUpperCase
  }

  var desc                                 = ""
  var version                              = "1.0.0"
  var validators: ListBuffer[Validator[T]] = ListBuffer()
  var overWritable: Boolean                = false

  def defaultBy(func: BitlapConf => T): BitlapConfKey[T] = this.also { it => it.defaultBy = func }

  def sys(systemProperty: String): BitlapConfKey[T] = this.also { it => it.sys = systemProperty }

  def env(envName: String): BitlapConfKey[T] = this.also { it => it.env = envName }

  def desc(description: String): BitlapConfKey[T] = this.also { it => it.desc = description }

  def version(version: String): BitlapConfKey[T] = this.also { it => it.version = version }

  def validator(v: Validator[T]): BitlapConfKey[T] = this.also { it => it.validators += v }

  def validators(v: Validator[T]*): BitlapConfKey[T] = this.also { it => it.validators.addAll(v) }

  def overWritable(o: Boolean): BitlapConfKey[T] = this.also { it => it.overWritable = o }

  override def toString: String =
    s"BitlapConfKey(key='$key', defaultValue=$defaultValue, type=${`type`.getName}, sys='$sys', env='$env', desc='$desc', version='$version')"
}

object BitlapConfKey {
  val cache: mutable.Map[String, BitlapConfKey[_]] = scala.collection.mutable.Map[String, BitlapConfKey[_]]()
}
