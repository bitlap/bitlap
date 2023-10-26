/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.spark

import org.apache.spark.sql.SparkSessionExtensions

/** spark extensions for bitlap
 */
class Extensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
//    extensions.injectPlannerStrategy()
  }
}
