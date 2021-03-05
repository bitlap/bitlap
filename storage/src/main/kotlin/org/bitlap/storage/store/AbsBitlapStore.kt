package org.bitlap.storage.store

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapProperties

/**
 * Desc:
 *
 * TODO: Add a store provider
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/23
 */
abstract class AbsBitlapStore<T>(conf: Configuration) : BitlapStore<T> {

    protected val rootPath = Path(BitlapProperties.getRootDir())
    protected var fs: FileSystem = rootPath.getFileSystem(conf).also {
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