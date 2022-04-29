/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
case class Metric(time: Long, entity: Int, dimensions: Option[List[Dimension]] = None, name: String, value: Int)
