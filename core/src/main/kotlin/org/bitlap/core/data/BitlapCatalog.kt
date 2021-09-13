package org.bitlap.core.data

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.common.LifeCycleWrapper
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.storage.metadata.DataSource
import org.bitlap.core.storage.store.DataSourceStore

/**
 * Desc: Catalog for schema, datasource, and etc.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/18
 */
open class BitlapCatalog(val conf: BitlapConf, val hadoopConf: Configuration) : LifeCycleWrapper() {

    companion object {
        const val DEFAULT_SCHEMA = "_default"
    }

    private val fs: FileSystem by lazy {
        rootPath.getFileSystem(hadoopConf).also {
            it.setWriteChecksum(false)
            it.setVerifyChecksum(false)
        }
    }
    private val rootPath by lazy {
        Path(conf.get(BitlapConf.DEFAULT_ROOT_DIR_DATA))
    }
    private val dataPath by lazy {
        Path(rootPath, "data")
    }

    override fun start() {
        super.start()
        if (!fs.exists(dataPath)) {
            fs.mkdirs(dataPath)
        }
    }

    fun createSchema(name: String, ifNotExists: Boolean = false): Boolean {
        val cleanName = PreConditions.checkNotBlank(name, "Schema").trim().lowercase()
        val p = Path(dataPath, cleanName)
        val exists = fs.exists(p)
        if (exists && ifNotExists) {
            return false
        } else if (exists) {
            throw BitlapException("Unable to create schema $cleanName, it already exists.")
        }
        fs.mkdirs(p)
        return true
    }

    fun dropSchema(name: String, cascade: Boolean = false): Boolean {
        val cleanName = PreConditions.checkNotBlank(name, "Schema").trim().lowercase()
        val p = Path(dataPath, cleanName)
        if (fs.exists(p)) {
            val files = fs.listStatus(p)
            if (!cascade && files.isNotEmpty()) {
                throw BitlapException("Unable to drop schema $cleanName, it's not empty, retry with cascade.")
            }
            fs.delete(p, cascade)
            return true
        }
        return false
    }

    fun renameSchema(from: String, to: String) {
        val cleanFrom = PreConditions.checkNotBlank(from, "Schema").trim().lowercase()
        val cleanTo = PreConditions.checkNotBlank(to, "Schema").trim().lowercase()
        val f = Path(dataPath, cleanFrom)
        val t = Path(dataPath, cleanTo)
        if (fs.exists(t)) {
            throw BitlapException("Unable to rename schema $cleanFrom to $cleanTo, schema $cleanTo already exists.")
        }
        if (fs.exists(f)) {
            fs.rename(f, t)
        }
    }

    fun getSchema(name: String): String {
        val cleanName = PreConditions.checkNotBlank(name, "Schema").trim().lowercase()
        val p = Path(dataPath, cleanName)
        if (!fs.exists(p)) {
            throw BitlapException("Unable to get schema $cleanName, it does not exist.")
        }
        return cleanName
    }

    fun listSchemas(): List<String> {
        return fs.listStatus(dataPath).asSequence()
            .filter { it.isDirectory }
            .map { it.path.name }
            .toList()
    }

    /**
     * create [DataSource] with [name].
     *
     * if [ifNotExists] is false, exception will be thrown when [DataSource] is exists
     * otherwise ignored.
     */
    fun createDataSource(name: String, schema: String = DEFAULT_SCHEMA, ifNotExists: Boolean = false) {
        val cleanSchema = PreConditions.checkNotBlank(schema, "Schema").trim().lowercase()
        val cleanName = PreConditions.checkNotBlank(name, "DataSource").trim().lowercase()
        val store = DataSourceStore(cleanName, cleanSchema, hadoopConf, conf)
        val ds = DataSource(cleanSchema, cleanName)
        val exists = store.exists()
        if (exists && ifNotExists) {
            return
        } else if (exists) {
            throw BitlapException("Unable to create DataSource $cleanSchema.$cleanName, it already exists.")
        }
        store.open()
        store.store(ds)
    }

    /**
     * get [DataSource] with [name]
     */
    fun getDataSource(name: String, schema: String = DEFAULT_SCHEMA): DataSource {
        val cleanSchema = PreConditions.checkNotBlank(schema, "Schema").trim().lowercase()
        val cleanName = PreConditions.checkNotBlank(name, "DataSource").trim().lowercase()
        val store = DataSourceStore(cleanName, cleanSchema, hadoopConf, conf)
        if (!store.exists()) {
            throw BitlapException("DataSource [$cleanSchema.$cleanName] is not exists.")
        }
        return store.get()
    }

    fun getDataSourceStore(name: String, schema: String = DEFAULT_SCHEMA): DataSourceStore {
        val cleanSchema = PreConditions.checkNotBlank(schema, "Schema").trim().lowercase()
        val cleanName = PreConditions.checkNotBlank(name, "DataSource").trim().lowercase()
        val store = DataSourceStore(cleanName, cleanSchema, hadoopConf, conf)
        if (!store.exists()) {
            throw BitlapException("DataSource [$name] is not exists.")
        }
        store.open()
        return store
    }
}
