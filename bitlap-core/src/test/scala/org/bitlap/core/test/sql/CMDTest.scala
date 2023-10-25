/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.sql

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class CMDTest extends BaseLocalFsTest with SqlChecker {

  test("test cmd sql") {
    checkRows(
      s"run example 'xxx'",
      List(List(s"hello 'xxx'"))
    )
  }
}
