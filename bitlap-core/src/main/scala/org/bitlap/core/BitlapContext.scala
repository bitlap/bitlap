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
package org.bitlap.core

import org.bitlap.common.{ BitlapConf, EventBus }
import org.bitlap.core.catalog.impl.BitlapCatalogImpl
import org.bitlap.core.sql.BitlapSqlPlanner

import org.apache.hadoop.conf.Configuration

/** Context with core components.
 */
object BitlapContext {

  val bitlapConf: BitlapConf    = BitlapConf()
  val hadoopConf: Configuration = Configuration() // TODO (merge bitlap.hadoop.xxx)

  lazy val eventBus: EventBus = {
    val e = EventBus()
    e.start()
    e
  }

  lazy val catalog: BitlapCatalogImpl = {
    val c = BitlapCatalogImpl(bitlapConf, hadoopConf)
    c.start()
    c
  }

  lazy val sqlPlanner: BitlapSqlPlanner = new BitlapSqlPlanner(catalog)
}
