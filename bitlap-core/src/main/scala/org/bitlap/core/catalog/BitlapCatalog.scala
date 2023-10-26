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
package org.bitlap.core.catalog

import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.catalog.metadata.Table

/** Catalog for schema, datasource, and etc.
 */
trait BitlapCatalog {

  /** Create [Database] with [name].
   *
   *  if [ifNotExists] is false, exception will be thrown when [Database] exists, otherwise ignored.
   */
  def createDatabase(name: String, ifNotExists: Boolean = false): Boolean

  /** Drop [Database] with [name].
   *
   *  [ifExists] if set false, exception will be thrown when [Database] does not exist, otherwise ignore. [cascade] if
   *  set true, it will drop all tables in the database.
   */
  def dropDatabase(name: String, ifExists: Boolean = false, cascade: Boolean = false): Boolean

  /** Rename database name.
   */
  def renameDatabase(from: String, to: String): Boolean

  /** Get [Database].
   */
  def getDatabase(name: String): Database

  /** Check if [name] is a valid database name.
   */
  def databaseExists(name: String): Boolean

  /** List all [Database], it also contains [Database.DEFAULT_DATABASE]
   */
  def listDatabases(): List[Database]

  /** create [Table] with [name] in the [database].
   *
   *  if [ifNotExists] is false, exception will be thrown when [Table] exists, otherwise ignore.
   */
  def createTable(name: String, database: String = Database.DEFAULT_DATABASE, ifNotExists: Boolean = false): Boolean

  /** Drop [Table] with [name] in the [database].
   *
   *  [ifExists] if set false, exception will be thrown when [Table] does not exist, otherwise ignore. [cascade] if set
   *  true, it will drop all data in the table.
   */
  def dropTable(
    name: String,
    database: String = Database.DEFAULT_DATABASE,
    ifExists: Boolean = false,
    cascade: Boolean = false
  ): Boolean

  /** get [Table] with [name] in the [database].
   */
  def getTable(name: String, database: String = Database.DEFAULT_DATABASE): Table

  /** List all [Table] in the [database].
   */
  def listTables(database: String = Database.DEFAULT_DATABASE): List[Table]

}
