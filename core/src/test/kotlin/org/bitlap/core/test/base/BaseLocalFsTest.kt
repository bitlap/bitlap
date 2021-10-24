package org.bitlap.core.test.base

import io.kotest.core.spec.style.StringSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.core.BitlapContext

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

    init {
        beforeSpec {
            val hadoopConf = Configuration()
            hadoopConf.set(FS_DEFAULT_NAME_KEY, "file:///")
            localFS = FileSystem.getLocal(hadoopConf)
            workPath = Path(localFS.workingDirectory, "target/bitlap-test/${it::class.simpleName}")
            if (localFS.exists(workPath)) {
                localFS.delete(workPath, true)
            }
            localFS.mkdirs(workPath)
            // set bitlap properties
            conf = BitlapContext.bitlapConf
            conf.set(BitlapConf.DEFAULT_ROOT_DIR_DATA, workPath.toString())
        }

        afterSpec {
            if (localFS.exists(workPath)) {
                localFS.delete(workPath, true)
            }
        }
    }
}
