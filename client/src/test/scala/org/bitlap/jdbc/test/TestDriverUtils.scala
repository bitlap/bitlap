/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc.test

import junit.framework.TestCase
import java.sql.DriverManager
import java.util.Properties

/**
 * @author 梦境迷离
 * @since 2021/8/28
 * @version 1.0
 */
class TestDriverUtils(name: String) extends TestCase(name) {

  private val driverName = "org.bitlap.jdbc.BitlapDriver"

  def test(): Unit = {
    Class.forName(driverName)
    val url = "jdbc:bitlap://localhost,127.0.0.1,192.168.1.1:23333/default"
    val driver = DriverManager.getDriver(url)
    val info = driver.getPropertyInfo(url, new Properties()).toList
    println(info.map { it =>
      s"name=[${it.name}], value=[${it.value}], description=[${it.description}], required=[${it.required}]"
    })
  }
}
