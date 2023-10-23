/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.catalog.impl

import org.bitlap.common.BitlapConf
import org.bitlap.common.LifeCycleWrapper
import org.bitlap.common.conf.BitlapConfKeys
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.BitlapContext
import org.bitlap.core.catalog.BitlapCatalog
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.event.DatabaseCreateEvent
import org.bitlap.core.event.DatabaseDeleteEvent
import org.bitlap.core.event.TableCreateEvent
import org.bitlap.core.event.TableDeleteEvent
import org.bitlap.core.hadoop._
import org.bitlap.core.storage.TableFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

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
  }

  /** Create [Database] with [name].
   *
   *  if [ifNotExists] is false, exception will be thrown when [Database] exists, otherwise ignored.
   */
  override def createDatabase(name: String, ifNotExists: Boolean): Boolean = {
    val cleanName = PreConditions.checkNotBlank(name, "database").trim().toLowerCase()
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
    return true
  }

  /** Drop [Database] with [name].
   *
   *  [ifExists] if set false, exception will be thrown when [Database] does not exist, otherwise ignore. [cascade] if
   *  set true, it will drop all tables in the database.
   */
  override def dropDatabase(name: String, ifExists: Boolean, cascade: Boolean): Boolean = {
    val cleanName = PreConditions.checkNotBlank(name, "database").trim().toLowerCase()
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
    return false
  }

  /** Rename database name.
   */
  override def renameDatabase(from: String, to: String): Boolean = {
    val cleanFrom = PreConditions.checkNotBlank(from, "database").trim().toLowerCase()
    if (cleanFrom == Database.DEFAULT_DATABASE) {
      throw BitlapException(s"Unable to rename default database, it's built-in.")
    }
    val cleanTo = PreConditions.checkNotBlank(to, "database").trim().toLowerCase()
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
    val cleanName = PreConditions.checkNotBlank(name, "database").trim().toLowerCase()
    val p         = Path(dataPath, cleanName)
    if (!fs.exists(p)) {
      throw BitlapException(s"Unable to get database $cleanName, it does not exist.")
    }
    return Database(cleanName)
  }

  /** Check if [name] is a valid database name.
   */
  override def databaseExists(name: String): Boolean = {
    val cleanName = PreConditions.checkNotBlank(name, "database").trim().toLowerCase()
    val p         = Path(dataPath, cleanName)
    return fs.exists(p)
  }

  /** List all [Database], it also contains [Database.DEFAULT_DATABASE]
   */
  override def listDatabases(): List[Database] = {
    return fs.listStatus(dataPath).filter(_.isDirectory).map(d => Database(d.getPath.getName)).toList
  }

  /** create [Table] with [name] in the [database].
   *
   *  if [ifNotExists] is false, exception will be thrown when [Table] exists, otherwise ignore.
   */
  override def createTable(name: String, database: String, ifNotExists: Boolean): Boolean = {
    val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().toLowerCase()
    val cleanName   = PreConditions.checkNotBlank(name, "table").trim().toLowerCase()
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
    val result = fs.writeTable(tableDir, table)
    eventBus.post(TableCreateEvent(table))
    result
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
    val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().toLowerCase()
    val cleanName   = PreConditions.checkNotBlank(name, "table").trim().toLowerCase()
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
    return false
  }

  /** get [Table] with [name] in the [database].
   */
  override def getTable(name: String, database: String): Table = {
    val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().toLowerCase()
    val cleanName   = PreConditions.checkNotBlank(name, "table").trim().toLowerCase()
    val tableDir    = Path(dataPath, s"$cleanDBName/$cleanName")
    if (!fs.exists(tableDir)) {
      throw BitlapException(s"Table $cleanDBName.$cleanName does not exist.")
    }
    return fs.readTable(tableDir)
  }

  /** List all [Table] in the [database].
   */
  override def listTables(database: String): List[Table] = {
    val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().toLowerCase()
    val dbDir       = Path(dataPath, cleanDBName)
    return fs
      .listStatus(dbDir) // TODO: par
      .filter(_.isDirectory)
      .map(d => fs.readTable(d.getPath))
      .toList
  }
}
