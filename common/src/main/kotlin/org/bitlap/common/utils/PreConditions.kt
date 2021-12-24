package org.bitlap.common.utils

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
object PreConditions {

    /**
     * check [o] cannot be null
     */
    @JvmStatic
    @JvmOverloads
    fun <T> checkNotNull(o: T?, key: String = "Object", msg: String = "$key cannot be null."): T {
        if (o == null) {
            throw NullPointerException(msg)
        }
        return o
    }

    /**
     * check [str] cannot be null or blank
     */
    @JvmStatic
    @JvmOverloads
    fun checkNotBlank(str: String?, key: String = "string", msg: String = "$key cannot be null or blank."): String {
        if (str.isNullOrBlank()) {
            throw IllegalArgumentException(msg)
        }
        return str
    }

    /**
     * check [collection] cannot be empty
     */
    @JvmStatic
    @JvmOverloads
    fun <T> checkNotEmpty(collection: Collection<T>?, key: String = "collection", msg: String = "$key cannot be empty."): Collection<T> {
        if (collection.isNullOrEmpty()) {
            throw IllegalArgumentException(msg)
        }
        return collection
    }

    /**
     * check [expr] cannot be false
     */
    @JvmStatic
    @JvmOverloads
    fun checkExpression(expr: Boolean, key: String = "expr", msg: String = "$key cannot be false") {
        if (!expr) {
            throw IllegalArgumentException(msg)
        }
    }
}
