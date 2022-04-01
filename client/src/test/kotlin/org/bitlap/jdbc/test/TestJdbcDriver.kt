/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc.test

import junit.framework.TestCase
import org.junit.Test
import java.sql.DriverManager
import java.sql.Statement

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
class TestJdbcDriver(name: String?) : TestCase(name) {

    private val driverName = "org.bitlap.jdbc.BitlapDriver"

    fun test() {
        Class.forName(driverName)
        val con = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default", "root", "root")
        assertNotNull("Connection is null", con)
        val stmt: Statement = con.createStatement()
        assertNotNull("Statement is null", stmt)

        // 执行SQL
        stmt.execute("select * from hello_table;")
        val resultSet = stmt.resultSet
        // 获取记录
        resultSet.next()
        // 获取第一行的列
        val id1 = resultSet.getInt(1)
        println(id1)

        val name1 = resultSet.getString(2)
        println(name1)

        val salary1 = resultSet.getDouble(3)
        println(salary1)

        // 获取第二行记录
        resultSet.next()
        // 获取第二行的列
        val id2 = resultSet.getInt(1)
        println(id2)

        val name2 = resultSet.getString(2)
        println(name2)

        val salary2 = resultSet.getDouble(3)
        println(salary2)

        val short = resultSet.getShort(4)
        println(short)

        val long = resultSet.getLong(5)
        println(long)

        val boolean = resultSet.getBoolean(6)
        println(boolean)

        val timestamp = resultSet.getTimestamp(7)
        println(timestamp)

        // 按列名获取第二行记录
        // 获取第二行的列
        val id3 = resultSet.getInt("ID")
        println(id3)

        val name3 = resultSet.getString("NAME")
        println(name3)

        val salary3 = resultSet.getDouble("SALARY")
        println(salary3)
    }

    @Test
    fun test2() {
        Class.forName(driverName)
        val con = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default", "root", "root")
        val stmt: Statement = con.createStatement()

        // 执行SQL
        stmt.execute("show tables in test_db")
        val rs = stmt.resultSet
        while (rs.next()) {
            println(rs.getString("table_name"))
        }
    }
}
