/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.catalog.impl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
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
import org.bitlap.core.storage.TableFormat
import org.bitlap.core.utils.Hcfs.readTable
import org.bitlap.core.utils.Hcfs.writeTable
import kotlin.streams.toList

/**
 * Impl for Catalog, using dfs to manage catalog metadata.
 */
open class BitlapCatalogImpl(private val conf: BitlapConf, private val hadoopConf: Configuration) : BitlapCatalog,
    LifeCycleWrapper() {

    private val fs: FileSystem by lazy {
        rootPath.getFileSystem(hadoopConf).also {
            it.setWriteChecksum(false)
            it.setVerifyChecksum(false)
        }
    }

    private val rootPath by lazy {
        Path(conf.get(BitlapConfKeys.ROOT_DIR))
    }

    private val dataPath by lazy {
        Path(rootPath, "data")
    }

    private val eventBus = BitlapContext.eventBus

    override fun start() {
        super.start()
        if (!fs.exists(dataPath)) {
            fs.mkdirs(dataPath)
        }
        val defaultDBPath = Path(dataPath, Database.DEFAULT_DATABASE)
        if (!fs.exists(defaultDBPath)) {
            fs.mkdirs(defaultDBPath)
        }
    }

    /**
     * Create [Database] with [name].
     *
     * if [ifNotExists] is false, exception will be thrown when [Database] exists, otherwise ignored.
     */
    override fun createDatabase(name: String, ifNotExists: Boolean): Boolean {
        val cleanName = PreConditions.checkNotBlank(name, "database").trim().lowercase()
        if (cleanName == Database.DEFAULT_DATABASE) {
            throw BitlapException("Unable to create default database, it's built-in.")
        }
        val p = Path(dataPath, cleanName)
        val exists = fs.exists(p)
        if (exists && ifNotExists) {
            return false
        } else if (exists) {
            throw BitlapException("Unable to create database $cleanName, it already exists.")
        }
        fs.mkdirs(p)
        eventBus.post(DatabaseCreateEvent(Database(cleanName)))
        return true
    }

    /**
     * Drop [Database] with [name].
     *
     * [ifExists] if set false, exception will be thrown when [Database] does not exist, otherwise ignore.
     * [cascade] if set true, it will drop all tables in the database.
     */
    override fun dropDatabase(name: String, ifExists: Boolean, cascade: Boolean): Boolean {
        val cleanName = PreConditions.checkNotBlank(name, "database").trim().lowercase()
        if (cleanName == Database.DEFAULT_DATABASE) {
            throw BitlapException("Unable to drop default database, it's built-in.")
        }
        val p = Path(dataPath, cleanName)
        val exists = fs.exists(p)
        if (exists) {
            val files = fs.listStatus(p).map { it.isDirectory }
            if (!cascade && files.isNotEmpty()) {
                throw BitlapException("Unable to drop database $cleanName, it's not empty, retry with cascade.")
            }
            fs.delete(p, true)
            eventBus.post(DatabaseDeleteEvent(Database(cleanName)))
            return true
        } else {
            if (!ifExists) {
                throw BitlapException("Unable to drop database $cleanName, it does not exist.")
            }
        }
        return false
    }

    /**
     * Rename database name.
     */
    override fun renameDatabase(from: String, to: String): Boolean {
        val cleanFrom = PreConditions.checkNotBlank(from, "database").trim().lowercase()
        if (cleanFrom == Database.DEFAULT_DATABASE) {
            throw BitlapException("Unable to rename default database, it's built-in.")
        }
        val cleanTo = PreConditions.checkNotBlank(to, "database").trim().lowercase()
        if (cleanTo == Database.DEFAULT_DATABASE) {
            throw BitlapException("Unable to rename to default database, it's built-in.")
        }
        val f = Path(dataPath, cleanFrom)
        val t = Path(dataPath, cleanTo)
        if (fs.exists(t)) {
            throw BitlapException("Unable to rename database $cleanFrom to $cleanTo, database $cleanTo already exists.")
        }
        if (fs.exists(f)) {
            fs.rename(f, t)
            return true
        }
        throw BitlapException("Unable to rename database $cleanFrom to $cleanTo, database $cleanFrom does not exist.")
    }

    /**
     * Get [Database].
     */
    override fun getDatabase(name: String): Database {
        val cleanName = PreConditions.checkNotBlank(name, "database").trim().lowercase()
        val p = Path(dataPath, cleanName)
        if (!fs.exists(p)) {
            throw BitlapException("Unable to get database $cleanName, it does not exist.")
        }
        return Database(cleanName)
    }

    /**
     * Check if [name] is a valid database name.
     */
    override fun databaseExists(name: String): Boolean {
        val cleanName = PreConditions.checkNotBlank(name, "database").trim().lowercase()
        val p = Path(dataPath, cleanName)
        return fs.exists(p)
    }

    /**
     * List all [Database], it also contains [Database.DEFAULT_DATABASE]
     */
    override fun listDatabases(): List<Database> {
        return fs.listStatus(dataPath).asSequence().filter { it.isDirectory }.map { Database(it.path.name) }.toList()
    }

    /**
     * create [Table] with [name] in the [database].
     *
     * if [ifNotExists] is false, exception will be thrown when [Table] exists, otherwise ignore.
     */
    override fun createTable(name: String, database: String, ifNotExists: Boolean): Boolean {
        val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().lowercase()
        val cleanName = PreConditions.checkNotBlank(name, "table").trim().lowercase()
        val tm = System.currentTimeMillis()
        val tableDir = Path(dataPath, "$cleanDBName/$cleanName")
        val table = Table(
            cleanDBName, cleanName, tm, tm,
            mutableMapOf(Table.TABLE_FORMAT_KEY to TableFormat.PARQUET.name),
            tableDir.toString(),
        )

        val exists = fs.exists(tableDir)
        if (exists && ifNotExists) {
            return false
        } else if (exists) {
            throw BitlapException("Unable to create table $cleanDBName.$cleanName, it already exists.")
        }
        return fs.writeTable(tableDir, table).apply {
            eventBus.post(TableCreateEvent(table))
        }
    }

    /**
     * Drop [Table] with [name] in the [database].
     *
     * [ifExists] if set false, exception will be thrown when [Table] does not exist, otherwise ignore.
     * [cascade] if set true, it will drop all data in the table.
     */
    override fun dropTable(name: String, database: String, ifExists: Boolean, cascade: Boolean): Boolean {
        val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().lowercase()
        val cleanName = PreConditions.checkNotBlank(name, "table").trim().lowercase()
        val tableDir = Path(dataPath, "$cleanDBName/$cleanName")
        val exists = fs.exists(tableDir)
        if (exists) {
            val files = fs.listStatus(tableDir).filter { it.isDirectory }
            if (!cascade && files.isNotEmpty()) {
                throw BitlapException("Unable to drop table $cleanDBName.$cleanName, it's not empty, retry with cascade.")
            }
            val table = this.getTable(cleanName, cleanDBName)
            fs.delete(tableDir, true)
            eventBus.post(TableDeleteEvent(table))
            return true
        } else {
            if (!ifExists) {
                throw BitlapException("Unable to drop table $cleanDBName.$cleanName, it does not exist.")
            }
        }
        return false
    }

    /**
     * get [Table] with [name] in the [database].
     */
    override fun getTable(name: String, database: String): Table {
        val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().lowercase()
        val cleanName = PreConditions.checkNotBlank(name, "table").trim().lowercase()
        val tableDir = Path(dataPath, "$cleanDBName/$cleanName")
        if (!fs.exists(tableDir)) {
            throw BitlapException("Table $cleanDBName.$cleanName does not exist.")
        }
        return fs.readTable(tableDir)
    }

    /**
     * List all [Table] in the [database].
     */
    override fun listTables(database: String): List<Table> {
        val cleanDBName = PreConditions.checkNotBlank(database, "database").trim().lowercase()
        val dbDir = Path(dataPath, cleanDBName)
        return fs.listStatus(dbDir).toList().parallelStream()
            .filter { it.isDirectory }
            .map { fs.readTable(it.path) }
            .toList()
    }
}
