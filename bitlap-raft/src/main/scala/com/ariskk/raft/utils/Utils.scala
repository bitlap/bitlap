/* Copyright (c) 2022 bitlap.org */
package com.ariskk.raft.utils

import java.util.UUID

object Utils {
  def newPrefixedId(prefix: String): String =
    s"$prefix-${UUID.randomUUID().toString.take(20)}"
}
