/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.hadoop

import java.io.FilterInputStream
import java.io.InputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PositionedReadable
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/** refer to hadoop AbstractHttpFileSystem
 */
abstract class AbstractHttpFileSystem(val schema: String) extends FileSystem {

  import AbstractHttpFileSystem._

  private var _uri: URI = _

  override def getScheme: String = this.schema

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    this._uri = name
  }

  override def getUri: URI = this._uri

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val pathUri = makeQualified(path).toUri
    val conn    = pathUri.toURL.openConnection()
    val input   = conn.getInputStream
    FSDataInputStream(HttpDataInputStream(input))
  }

  override def create(
    f: Path,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): FSDataOutputStream = {
    throw UnsupportedOperationException()
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throw UnsupportedOperationException()

  override def rename(src: Path, dst: Path): Boolean = throw UnsupportedOperationException()

  override def delete(f: Path, recursive: Boolean): Boolean = throw UnsupportedOperationException()

  override def listStatus(path: Path): Array[FileStatus] = throw UnsupportedOperationException()

  override def setWorkingDirectory(new_dir: Path): Unit = {}

  override def getWorkingDirectory: Path = WORKING_DIR

  override def mkdirs(path: Path, permission: FsPermission): Boolean = false

  override def getFileStatus(path: Path): FileStatus =
    FileStatus(-1, false, 1, DEFAULT_BLOCK_SIZE, 0, makeQualified(path))
}

private class HttpDataInputStream(input: InputStream)
    extends FilterInputStream(input)
    with Seekable
    with PositionedReadable {

  override def read(
    position: Long,
    buffer: Array[Byte],
    offset: Int,
    length: Int
  ): Int = {
    throw UnsupportedOperationException()
  }

  override def readFully(
    position: Long,
    buffer: Array[Byte],
    offset: Int,
    length: Int
  ): Unit = {
    throw UnsupportedOperationException()
  }

  override def readFully(position: Long, buffer: Array[Byte]): Unit = {
    throw UnsupportedOperationException()
  }

  override def seek(pos: Long): Unit = {
    throw UnsupportedOperationException()
  }

  override def getPos: Long = {
    throw UnsupportedOperationException()
  }

  override def seekToNewSource(targetPos: Long): Boolean = {
    throw UnsupportedOperationException()
  }
}

object AbstractHttpFileSystem {
  private val DEFAULT_BLOCK_SIZE = 4096L
  private val WORKING_DIR        = Path("/")
}
