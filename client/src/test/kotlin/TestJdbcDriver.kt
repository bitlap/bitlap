
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
    }
}
