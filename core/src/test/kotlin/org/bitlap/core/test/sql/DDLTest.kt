package org.bitlap.core.test.sql

import io.kotest.matchers.shouldBe
import org.bitlap.core.sql.QueryExecution
import org.bitlap.core.test.base.BaseLocalFsTest

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/14
 */
class DDLTest : BaseLocalFsTest() {

    init {
        "show databases" {
            val schema = "test_database"
            var rs = QueryExecution("create database $schema").execute()
            rs.use {
                while (rs.next()) {
                    rs.getBoolean(1) shouldBe true
                }
            }
            rs = QueryExecution("show databases").execute()
            rs.use {
                while (rs.next()) {
                    rs.getString(1) shouldBe schema
                }
            }
        }
    }
}
