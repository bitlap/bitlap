/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.csv

import org.bitlap.tools.{ builder, toString }

import scala.reflect.ClassTag

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/28
 */
@toString
@builder
case class CsvParserSetting[T](fileName: String, dimensionName: String, classTag: ClassTag[T])
