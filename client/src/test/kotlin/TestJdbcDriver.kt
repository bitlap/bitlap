import junit.framework.TestCase
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
        val con = DriverManager.getConnection("jdbc:bitlap://127.0.0.1:23333/default", "", "")
        assertNotNull("Connection is null", con)
        val stmt: Statement = con.createStatement()
        assertNotNull("Statement is null", stmt)
    }
}
