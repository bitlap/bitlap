/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PositionedReadable
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import java.io.FilterInputStream
import java.io.InputStream
import java.net.URI

/**
 * Desc: refer to hadoop AbstractHttpFileSystem
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2022/11/23
 */
abstract class AbstractHttpFileSystem(val schema: String) : FileSystem() {

    companion object {
        const val DEFAULT_BLOCK_SIZE = 4096L
        val WORKING_DIR = Path("/")
    }

    private lateinit var _uri: URI

    override fun getScheme(): String {
        return this.schema
    }

    override fun initialize(name: URI?, conf: Configuration?) {
        super.initialize(name, conf)
        this._uri = name!!
    }

    override fun getUri(): URI {
        return this._uri
    }

    override fun open(path: Path?, bufferSize: Int): FSDataInputStream {
        val pathUri = makeQualified(path).toUri()
        val conn = pathUri.toURL().openConnection()
        val input = conn.getInputStream()
        return FSDataInputStream(HttpDataInputStream(input))
    }

    override fun create(
        f: Path?,
        permission: FsPermission?,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable?
    ): FSDataOutputStream {
        throw UnsupportedOperationException()
    }

    override fun append(f: Path?, bufferSize: Int, progress: Progressable?): FSDataOutputStream =
        throw UnsupportedOperationException()

    override fun rename(src: Path?, dst: Path?): Boolean = throw UnsupportedOperationException()

    override fun delete(f: Path?, recursive: Boolean): Boolean = throw UnsupportedOperationException()

    override fun listStatus(path: Path?): Array<FileStatus> = throw UnsupportedOperationException()

    override fun setWorkingDirectory(new_dir: Path?) {}

    override fun getWorkingDirectory(): Path = WORKING_DIR

    override fun mkdirs(path: Path?, permission: FsPermission?): Boolean = false

    override fun getFileStatus(path: Path?): FileStatus =
        FileStatus(-1, false, 1, DEFAULT_BLOCK_SIZE, 0, makeQualified(path))
}

private class HttpDataInputStream(input: InputStream) : FilterInputStream(input), Seekable, PositionedReadable {
    override fun read(position: Long, buffer: ByteArray?, offset: Int, length: Int): Int {
        throw UnsupportedOperationException()
    }

    override fun readFully(position: Long, buffer: ByteArray?, offset: Int, length: Int) {
        throw UnsupportedOperationException()
    }

    override fun readFully(position: Long, buffer: ByteArray?) {
        throw UnsupportedOperationException()
    }

    override fun seek(pos: Long) {
        throw UnsupportedOperationException()
    }

    override fun getPos(): Long {
        throw UnsupportedOperationException()
    }

    override fun seekToNewSource(targetPos: Long): Boolean {
        throw UnsupportedOperationException()
    }
}
