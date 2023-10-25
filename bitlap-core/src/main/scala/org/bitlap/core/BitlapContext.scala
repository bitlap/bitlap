/**
 * Copyright (C) 2023 bitlap.org .
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
