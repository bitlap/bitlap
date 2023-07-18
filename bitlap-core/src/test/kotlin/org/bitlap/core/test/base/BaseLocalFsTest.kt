/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.test.base

import io.kotest.core.spec.style.StringSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.common.conf.BitlapConfKeys
import org.bitlap.common.utils.RandomEx
import org.bitlap.core.BitlapContext
import org.joda.time.DateTime

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/25
 */
@Suppress("BlockingMethodInNonBlockingContext")
abstract class BaseLocalFsTest : StringSpec() {
    protected lateinit var workPath: Path
    protected lateinit var localFS: FileSystem
    protected lateinit var conf: BitlapConf
    protected lateinit var hadoopConf: Configuration

    init {
        beforeSpec {
            hadoopConf = BitlapContext.hadoopConf
            hadoopConf.set(FS_DEFAULT_NAME_KEY, "file:///")
            localFS = FileSystem.getLocal(hadoopConf)
            workPath = Path(localFS.workingDirectory, "target/bitlap-test")
            if (!localFS.exists(workPath)) {
                localFS.mkdirs(workPath)
            }
            // set bitlap properties
            conf = BitlapContext.bitlapConf
            conf.set(BitlapConfKeys.ROOT_DIR.key, workPath.toString(), true)
        }

        afterSpec {
        }

        afterProject {
            if (localFS.exists(workPath)) {
                localFS.delete(workPath, true)
            }
        }
    }

    protected fun randomDBTable(): Pair<String, String> = randomDatabase() to randomTable()
    protected fun randomDatabase(): String {
        val tm = DateTime.now().toString("yyyyMMddHHmmss")
        return "database_${tm}_${RandomEx.string(5)}"
    }
    protected fun randomTable(): String {
        val tm = DateTime.now().toString("yyyyMMddHHmmss")
        return "table_${tm}_${RandomEx.string(5)}"
    }
}
