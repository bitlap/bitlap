/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.base

import org.bitlap.common.BitlapConf
import org.bitlap.common.conf.BitlapConfKeys
import org.bitlap.common.utils.RandomEx
import org.bitlap.core.BitlapContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

import flatspec._
import matchers._

trait BaseLocalFsTest extends AnyFunSuite with BeforeAndAfterAll with should.Matchers with Inspectors {
  protected var workPath: Path            = _
  protected var localFS: FileSystem       = _
  protected var conf: BitlapConf          = _
  protected var hadoopConf: Configuration = _

  override protected def beforeAll(): Unit = {
    hadoopConf = BitlapContext.hadoopConf
    hadoopConf.set(FS_DEFAULT_NAME_KEY, "file:///")
    localFS = FileSystem.getLocal(hadoopConf)
    workPath = Path(localFS.getWorkingDirectory, "target/bitlap-test")
    if (!localFS.exists(workPath)) {
      localFS.mkdirs(workPath)
    }
    // set bitlap properties
    conf = BitlapContext.bitlapConf
    conf.set(BitlapConfKeys.ROOT_DIR.getKey, workPath.toString, true)
  }

  override protected def afterAll(): Unit = {
    if (localFS.exists(workPath)) {
//      localFS.delete(workPath, true)
    }
  }

  protected def randomDBTable(): (String, String) = randomDatabase() -> randomTable()

  protected def randomDatabase(): String = {
    val tm = DateTime.now().toString("yyyyMMddHHmmss")
    s"database_${tm}_${RandomEx.string(5)}"
  }

  protected def randomTable(): String = {
    val tm = DateTime.now().toString("yyyyMMddHHmmss")
    s"table_${tm}_${RandomEx.string(5)}"
  }
}
