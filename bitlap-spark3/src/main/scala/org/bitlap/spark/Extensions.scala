/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark

import org.apache.spark.sql.SparkSessionExtensions

/** Desc: spark extensions for bitlap
 */
class Extensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
//    extensions.injectPlannerStrategy()
  }
}