package org.bitlap.core.storage.store

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.core.storage.BitlapStore

/**
 * Desc:
 *
 * TODO: Add a store provider
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/23
 */
abstract class AbsBitlapStore<T>(hadoopConf: Configuration, conf: BitlapConf) : BitlapStore<T> {

    protected val projectName = conf.get(BitlapConf.PROJECT_NAME)
    protected val rootPath = conf.get(BitlapConf.DEFAULT_ROOT_DIR_DATA)
    protected var fs: FileSystem = Path(rootPath).getFileSystem(hadoopConf).also {
        it.setWriteChecksum(false)
        it.setVerifyChecksum(false)
    }
    protected abstract val dataDir: Path

    override fun open() {
        if (!fs.exists(dataDir)) {
            fs.mkdirs(dataDir)
        }
    }
}
