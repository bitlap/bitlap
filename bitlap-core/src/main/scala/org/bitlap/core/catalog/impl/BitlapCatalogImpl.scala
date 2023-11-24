/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.catalog.impl

import java.nio.file.Paths

import scala.jdk.CollectionConverters.*
import scala.util.Using

import org.bitlap.common.BitlapConf
import org.bitlap.common.LifeCycleWrapper
import org.bitlap.common.conf.BitlapConfKeys
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.{ JsonUtil, PreConditions }
import org.bitlap.core.BitlapContext
import org.bitlap.core.catalog.BitlapCatalog
import org.bitlap.core.catalog.metadata.{ Account, Database, Table }
import org.bitlap.core.event.*
import org.bitlap.core.hadoop.*
import org.bitlap.core.storage.TableFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, LocatedFileStatus, Path }
import org.apache.hadoop.hdfs.protocol.HdfsNamedFileStatus

/** Impl for Catalog, using dfs to manage catalog metadata.
 */
class BitlapCatalogImpl(private val conf: BitlapConf, private val hadoopConf: Configuration)
    extends LifeCycleWrapper
    with BitlapCatalog {

  private lazy val fs: FileSystem = {
    val _fs = rootPath.getFileSystem(hadoopConf)
    _fs.setWriteChecksum(false)
    _fs.setVerifyChecksum(false)
    _fs
  }

  private lazy val rootPath = Path(conf.get(BitlapConfKeys.ROOT_DIR))

  private lazy val dataPath = Path(rootPath, "data")

  private val eventBus = BitlapContext.eventBus

  override def start(): Unit = {
    super.start()
    if (!fs.exists(dataPath)) {
      fs.mkdirs(dataPath)
    }
    val defaultDBPath = Path(dataPath, Database.DEFAULT_DATABASE)
    if (!fs.exists(defaultDBPath)) {
      fs.mkdirs(defaultDBPath)
    }

    val defaultAccountPath = Path(getAccountDir, Account.DEFAULT_USER)
    if (!fs.exists(defaultAccountPath)) {
      fs.mkdirs(defaultAccountPath)
      fs.writeSecret(defaultAccountPath, Account.SecretKey()) {}
    }
  }

  /** Create [Database] with [name].
   *
   *  if [ifNotExists] is false, exception will be thrown when [Database] exists, otherwise ignored.
   */
  override def createDatabase(name: String, ifNotExists: Boolean): Boolean = {
    val cleanName = cleanDatabaseName(name)
    if (cleanName == Database.DEFAULT_DATABASE) {
      throw BitlapException(s"Unable to create default database, it's built-in.")
    }
    val p      = Path(dataPath, cleanName)
    val exists = fs.exists(p)
    if (exists && ifNotExists) {
      return false
    } else if (exists) {
      throw BitlapException(s"Unable to create database $cleanName, it already exists.")
    }
    fs.mkdirs(p)
    eventBus.post(DatabaseCreateEvent(Database(cleanName)))
    true
  }

  /** Drop [Database] with [name].
   *
   *  [ifExists] if set false, exception will be thrown when [Database] does not exist, otherwise ignore. [cascade] if
   *  set true, it will drop all tables in the database.
   */
  override def dropDatabase(name: String, ifExists: Boolean, cascade: Boolean): Boolean = {
    val cleanName = cleanDatabaseName(name)
    if (cleanName == Database.DEFAULT_DATABASE) {
      throw BitlapException(s"Unable to drop default database, it's built-in.")
    }
    val p      = Path(dataPath, cleanName)
    val exists = fs.exists(p)
    if (exists) {
      val files = fs.listStatus(p).map(_.isDirectory)
      if (!cascade && files.nonEmpty) {
        throw BitlapException(s"Unable to drop database $cleanName, it's not empty, retry with cascade.")
      }
      fs.delete(p, true)
      eventBus.post(DatabaseDeleteEvent(Database(cleanName)))
      return true
    } else {
      if (!ifExists) {
        throw BitlapException(s"Unable to drop database $cleanName, it does not exist.")
      }
    }
    false
  }

  /** Rename database name.
   */
  override def renameDatabase(from: String, to: String): Boolean = {
    val cleanFrom = cleanDatabaseName(from)
    if (cleanFrom == Database.DEFAULT_DATABASE) {
      throw BitlapException(s"Unable to rename default database, it's built-in.")
    }
    val cleanTo = cleanDatabaseName(to)
    if (cleanTo == Database.DEFAULT_DATABASE) {
      throw BitlapException(s"Unable to rename to default database, it's built-in.")
    }
    val f = Path(dataPath, cleanFrom)
    val t = Path(dataPath, cleanTo)
    if (fs.exists(t)) {
      throw BitlapException(s"Unable to rename database $cleanFrom to $cleanTo, database $cleanTo already exists.")
    }
    if (fs.exists(f)) {
      fs.rename(f, t)
      return true
    }
    throw BitlapException(s"Unable to rename database $cleanFrom to $cleanTo, database $cleanFrom does not exist.")
  }

  /** Get [Database].
   */
  override def getDatabase(name: String): Database = {
    val cleanName = cleanDatabaseName(name)
    val p         = Path(dataPath, cleanName)
    if (!fs.exists(p)) {
      throw BitlapException(s"Unable to get database $cleanName, it does not exist.")
    }
    Database(cleanName)
  }

  /** Check if [name] is a valid database name.
   */
  override def databaseExists(name: String): Boolean = {
    val cleanName = cleanDatabaseName(name)
    val p         = Path(dataPath, cleanName)
    fs.exists(p)
  }

  /** List all [Database], it also contains [Database.DEFAULT_DATABASE]
   */
  override def listDatabases(): List[Database] = {
    fs.collectStatus(dataPath, _.isDirectory) { (fs, status) =>
      Database(status.getPath.getName)
    }
  }

  /** create [Table] with [name] in the [database].
   *
   *  if [ifNotExists] is false, exception will be thrown when [Table] exists, otherwise ignore.
   */
  override def createTable(name: String, database: String, ifNotExists: Boolean): Boolean = {
    val cleanDBName = cleanDatabaseName(database)
    val cleanName   = cleanTableName(name)
    val tm          = System.currentTimeMillis()
    val tableDir    = Path(dataPath, s"$cleanDBName/$cleanName")
    val table = Table(
      cleanDBName,
      cleanName,
      tm,
      tm,
      scala.collection.mutable.Map(Table.TABLE_FORMAT_KEY -> TableFormat.PARQUET.name),
      tableDir.toString
    )

    val exists = fs.exists(tableDir)
    if (exists && ifNotExists) {
      return false
    } else if (exists) {
      throw BitlapException(s"Unable to create table $cleanDBName.$cleanName, it already exists.")
    }
    fs.writeTable(tableDir, table) {
      eventBus.post(TableCreateEvent(table))
    }
  }

  /** Drop [Table] with [name] in the [database].
   *
   *  [ifExists] if set false, exception will be thrown when [Table] does not exist, otherwise ignore. [cascade] if set
   *  true, it will drop all data in the table.
   */
  override def dropTable(
    name: String,
    database: String,
    ifExists: Boolean,
    cascade: Boolean
  ): Boolean = {
    val cleanDBName = cleanDatabaseName(database)
    val cleanName   = cleanTableName(name)
    val tableDir    = Path(dataPath, s"$cleanDBName/$cleanName")
    val exists      = fs.exists(tableDir)
    if (exists) {
      val files = fs.listStatus(tableDir).filter(_.isDirectory)
      if (!cascade && files.nonEmpty) {
        throw BitlapException(s"Unable to drop table $cleanDBName.$cleanName, it's not empty, retry with cascade.")
      }
      val table = this.getTable(cleanName, cleanDBName)
      fs.delete(tableDir, true)
      eventBus.post(TableDeleteEvent(table))
      return true
    } else {
      if (!ifExists) {
        throw BitlapException(s"Unable to drop table $cleanDBName.$cleanName, it does not exist.")
      }
    }
    false
  }

  /** get [Table] with [name] in the [database].
   */
  override def getTable(name: String, database: String): Table = {
    val cleanDBName = cleanDatabaseName(database)
    val cleanName   = cleanTableName(name)
    val tableDir    = Path(dataPath, s"$cleanDBName/$cleanName")
    if (!fs.exists(tableDir)) {
      throw BitlapException(s"Table $cleanDBName.$cleanName does not exist.")
    }
    fs.readTable(tableDir)
  }

  /** List all [Table] in the [database].
   */
  override def listTables(database: String): List[Table] = {
    val cleanDBName = cleanDatabaseName(database)
    val dbDir       = Path(dataPath, cleanDBName)
    // TODO: par
    fs.collectStatus(dbDir, _.isDirectory) { (_, status) =>
      fs.readTable(status.getPath)
    }
  }

  override def listUsers: List[Account] = {
    fs.collectStatus(getAccountDir, _.isDirectory) { (_, status) =>
      Account(status.getPath.getName)
    }
  }

  override def dropUser(username: String, ifExists: Boolean): Boolean = {
    val cleanName = cleanUserName(username)
    if (cleanName == Account.DEFAULT_USER) {
      throw BitlapException(s"Unable to drop default user, it's built-in.")
    }
    val p      = Path(getAccountDir, cleanName)
    val exists = fs.exists(p)
    if (exists) {
      fs.delete(p, true)
      eventBus.post(AccountDropEvent(Account(cleanName)))
      return true
    } else {
      if (!ifExists) {
        throw BitlapException(s"Unable to drop user $cleanName, it does not exist.")
      }
    }
    false
  }

  override def createUser(username: String, password: String, ifNotExists: Boolean): Boolean = {
    val cleanName = cleanUserName(username)
    if (cleanName == Account.DEFAULT_USER) {
      throw BitlapException(s"Unable to create default user, it's built-in.")
    }
    val p      = Path(getAccountDir, cleanName)
    val exists = fs.exists(p)
    if (exists && ifNotExists) {
      return false
    } else if (exists) {
      throw BitlapException(s"Unable to create user $cleanName, it already exists.")
    }
    fs.mkdirs(p)
    val secret = Account.SecretKey(password)
    fs.writeSecret(p, secret) {
      eventBus.post(AccountCreateEvent(Account(cleanName, secret)))
    }
  }

  private inline def cleanDatabaseName(database: String): String = {
    PreConditions.checkNotBlank(database, "database").trim().toLowerCase()
  }

  private inline def cleanTableName(tableName: String): String = {
    PreConditions.checkNotBlank(tableName, "table").trim().toLowerCase()
  }

  private inline def cleanUserName(username: String): String = {
    PreConditions.checkNotBlank(username, "username").trim().toLowerCase()
  }

  private inline def getAccountDir = Path.mergePaths(rootPath, Path(Account.DEFAULT_DIR))
}
