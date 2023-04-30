/* Copyright (c) 2023 bitlap.org */
package org.bitlap

import org.bitlap.jdbc.BitlapDriver

/** @author
 *    梦境迷离
 *  @version 1.0,2023/4/30
 */
final class Driver extends BitlapDriver

object Driver:
  val _ = (new Driver()).register();
