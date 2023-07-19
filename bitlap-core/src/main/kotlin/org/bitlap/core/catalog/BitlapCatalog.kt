/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.catalog

import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.catalog.metadata.Table

/**
 * Desc: Catalog for schema, datasource, and etc.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/18
 */
interface BitlapCatalog {

    /**
     * Create [Database] with [name].
     *
     * if [ifNotExists] is false, exception will be thrown when [Database] exists, otherwise ignored.
     */
    fun createDatabase(name: String, ifNotExists: Boolean = false): Boolean

    /**
     * Drop [Database] with [name].
     *
     * [ifExists] if set false, exception will be thrown when [Database] does not exist, otherwise ignore.
     * [cascade] if set true, it will drop all tables in the database.
     */
    fun dropDatabase(name: String, ifExists: Boolean = false, cascade: Boolean = false): Boolean

    /**
     * Rename database name.
     */
    fun renameDatabase(from: String, to: String): Boolean

    /**
     * Get [Database].
     */
    fun getDatabase(name: String): Database

    /**
     * Check if [name] is a valid database name.
     */
    fun databaseExists(name: String): Boolean

    /**
     * List all [Database], it also contains [Database.DEFAULT_DATABASE]
     */
    fun listDatabases(): List<Database>

    /**
     * create [Table] with [name] in the [database].
     *
     * if [ifNotExists] is false, exception will be thrown when [Table] exists, otherwise ignore.
     */
    fun createTable(name: String, database: String = Database.DEFAULT_DATABASE, ifNotExists: Boolean = false): Boolean

    /**
     * Drop [Table] with [name] in the [database].
     *
     * [ifExists] if set false, exception will be thrown when [Table] does not exist, otherwise ignore.
     * [cascade] if set true, it will drop all data in the table.
     */
    fun dropTable(name: String, database: String = Database.DEFAULT_DATABASE, ifExists: Boolean = false, cascade: Boolean = false): Boolean

    /**
     * get [Table] with [name] in the [database].
     */
    fun getTable(name: String, database: String = Database.DEFAULT_DATABASE): Table

    /**
     * List all [Table] in the [database].
     */
    fun listTables(database: String = Database.DEFAULT_DATABASE): List<Table>
}