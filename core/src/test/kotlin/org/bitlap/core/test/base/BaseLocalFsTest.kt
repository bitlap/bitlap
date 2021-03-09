package org.bitlap.core.test.base

import io.kotest.core.spec.style.StringSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapProperties
import org.bitlap.common.BitlapProperties.DEFAULT_ROOT_DIR

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/25
 */
@Suppress("BlockingMethodInNonBlockingContext")
abstract class BaseLocalFsTest : StringSpec() {
    protected lateinit var workPath: Path
    protected lateinit var localFS: FileSystem

    init {
        beforeSpec {
            val conf = Configuration()
            conf.set(FS_DEFAULT_NAME_KEY, "file:///")
            localFS = FileSystem.getLocal(conf)
            workPath = Path(localFS.workingDirectory, "target/bitlap-test")
            if (localFS.exists(workPath)) {
                localFS.delete(workPath, true)
            }
            localFS.mkdirs(workPath)
            // set bitlap properties
            BitlapProperties.setDefault(DEFAULT_ROOT_DIR, workPath.toString())
            BitlapProperties.init()
        }

        afterSpec {
        }
    }
}
