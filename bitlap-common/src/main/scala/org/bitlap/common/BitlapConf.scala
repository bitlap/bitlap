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
package org.bitlap.common

import java.io.Serializable

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

import org.bitlap.common.conf.BitlapConfKey
import org.bitlap.common.utils.{ JsonUtil, PreConditions }

import org.slf4j.LoggerFactory

import com.typesafe.config.{ ConfigFactory, ConfigParseOptions, ConfigResolveOptions, ConfigValue }

/** Bitlap core configuration.
 *
 *  [[BitlapConfKey]] is designed as follows, and the priority is the same as below:
 *
 *    - with `sys` to get value from java system properties, default is `bitlap.${name}`
 *    - with `env` to get value from OS environment variables, default is `BITLAP_${upper_trans_dot(name)}`
 *    - with `name` to get value from `bitlap.conf` configuration
 *    - `default` value
 *    - value `data type`
 *    - value `validator`
 *    - conf key `version`
 *    - ......
 */
class BitlapConf(conf: Map[String, String] = Map.empty) extends Serializable {

  private val log = LoggerFactory.getLogger(classOf[BitlapConf])

  /** core properties
   */
  private val props = {
    val config =
      try {
        ConfigFactory.load("bitlap", ConfigParseOptions.defaults(), ConfigResolveOptions.defaults())
      } catch {
        case e: Exception =>
          log.warn(s"Loading bitlap.conf error, cause: ${e.getMessage}")
          ConfigFactory.empty()
      }
    val map = scala.collection.mutable.Map.newBuilder[String, String]

    config.entrySet().asScala.foreach { e =>
      if (e.getKey.startsWith("bitlap.") && e.getValue.unwrapped() != null) {

        map.addOne(e.getKey -> e.getValue.unwrapped().toString)
      }
    }

    // load java system properties
    System.getProperties.asScala.collect {
      case (key, value) if key.startsWith("bitlap.") =>
        map.addOne(key -> value)
    }

    // load OS environment variablesR
    System.getenv().asScala.collect {
      case (key, value) if key.startsWith("BITLAP_") =>
        map.addOne(key.replace("_", ".").toLowerCase -> value)
    }
    map.result()
  }

  {
    // force merge props
    conf.foreach { case (key, value) =>
      this.set(key, value, true)
    }
  }

  def set(key: String, value: String = "", forceOverwrite: Boolean = false): String = synchronized {
    val confKey = BitlapConfKey.cache(key)
    if (confKey != null) {
      if (confKey.overWritable || forceOverwrite) {
        this.props.put(key, value)
      } else {
        throw IllegalArgumentException(s"$key cannot be overwrite.")
      }
    } else {
      this.props.put(key, value)
    }
    return value
  }

  def clone(other: Map[String, String] = Map.empty): BitlapConf = {
    val thisConf = BitlapConf(this.props.toMap)
    other.foreach { case (k, v) => thisConf.set(k, v) }
    thisConf
  }

  def toJson: String = JsonUtil.json(this.props)

  override def toString: String = this.props.toString()

  def get[T](confKey: BitlapConfKey[T]): T = {

    val result = this.props.get(confKey.key).map(_.trim) match {
      case None => confKey.defaultBy(this)
      case Some(value) =>
        (confKey.`type` match {
          case c if c == classOf[String]                                     => value
          case c if c == classOf[Byte] || c == classOf[java.lang.Byte]       => value.toByteOption.orNull
          case c if c == classOf[Short] || c == classOf[java.lang.Short]     => value.toShortOption.orNull
          case c if c == classOf[Int] || c == classOf[java.lang.Integer]     => value.toIntOption.orNull
          case c if c == classOf[Long] || c == classOf[java.lang.Long]       => value.toLongOption.orNull
          case c if c == classOf[Float] || c == classOf[java.lang.Float]     => value.toFloatOption.orNull
          case c if c == classOf[Double] || c == classOf[java.lang.Double]   => value.toDoubleOption.orNull
          case c if c == classOf[Char] || c == classOf[java.lang.Character]  => value.headOption.orNull
          case c if c == classOf[Boolean] || c == classOf[java.lang.Boolean] => value.toBooleanOption.orNull
          case c if c == classOf[Duration]                                   => Duration(value)
          case _ => throw IllegalArgumentException(s"Illegal value type: ${confKey.`type`}")
        }).asInstanceOf[T]
    }

    confKey.validators.foreach { validator =>
      PreConditions.checkExpression(validator.validate(result), msg = s"Value of [$confKey] is invalid.")
    }
    result
  }

  /** get milliseconds from a duration config
   */
  def getMillis(confKey: BitlapConfKey[Duration]): Long = {
    this.get[Duration](confKey).toMillis
  }

  def reload(): Unit = synchronized {
    // TODO
  }
}
