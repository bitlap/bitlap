/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.common.exception.BitlapException

/**
 * Abstract store
 */
abstract class AbsBitlapStore<T>(val storePath: Path, hadoopConf: Configuration) : BitlapStore<T> {

    protected var fs: FileSystem = storePath.getFileSystem(hadoopConf).also {
        it.setWriteChecksum(false)
        it.setVerifyChecksum(false)
    }

    override fun open() {
        if (!fs.exists(storePath)) {
            throw BitlapException("Unable to open store: $storePath, it does not exist.")
        }
    }
}
