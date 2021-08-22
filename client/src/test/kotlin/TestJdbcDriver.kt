import java.sql.DriverManager
import java.sql.Statement
import junit.framework.TestCase

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

        stmt.execute("select * from hello_table;")
        val resultSet = stmt.resultSet
        val ret = mutableListOf<String>()
        var i = 0
        while (resultSet.next() && i < 6) {
            i += 1
            val row1 = resultSet.getString(1)
            ret.add(row1)
        }
        assert(ret.toList() == listOf("hello", "world", "nice", "to", "meet", "you"))
    }
}
