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
abstract class AbsBitlapStore<T> : BitlapStore<T> {

    protected val rootPath = Path(BitlapProperties.getRootDir())
    protected var fs: FileSystem

    constructor(conf: Configuration) {
        fs = rootPath.getFileSystem(conf)
    }

}