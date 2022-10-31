package org.bitlap.testkit;

import org.bitlap.testkit.server.EmbedBitlapServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 梦境迷离
 * @version 1.0, 2022/10/24
 */
public class JavaServerTest {

    static {
        try {
            Class.forName(org.bitlap.Driver.class.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    final static String table = "test_table" + FakeDataUtil.randEntityNumber();

    @BeforeClass
    public static void startServer() throws SQLException, InterruptedException {
        Thread server = new Thread(() -> EmbedBitlapServer.main(new String[0]));
        server.setDaemon(true);
        server.start();
        Thread.sleep(3000L);
        initTable();
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        Statement stmt = conn().createStatement();
        stmt.execute("drop table " + table + " cascade");
    }


    private static void initTable() throws SQLException {
        Statement stmt = conn().createStatement();
        stmt.execute("create table if not exists " + table);
        stmt.execute("load data 'classpath:simple_data.csv' overwrite table " + table); // load的是server模块的csv
    }

    public static Connection conn() throws SQLException {
        return DriverManager.getConnection("jdbc:bitlap://localhost:23333/default");
    }

    public static class Tuple4 {
        private Long col1;
        private Double col2;
        private Double col3;
        private Long col4;

        public Tuple4(Long col1, Double col2, Double col3, Long col4) {
            this.col1 = col1;
            this.col2 = col2;
            this.col3 = col3;
            this.col4 = col4;
        }
    }

    @Test
    public void query_test1() throws SQLException {
        Statement stmt = conn().createStatement();
        stmt.setMaxRows(10);
        stmt.execute("select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv " + "  from " + table + "   where _time >= 0 " + " group by _time");
        ResultSet rs = stmt.getResultSet();
        List<Tuple4> ret = new ArrayList<>();
        if (rs != null) {
            while (rs.next()) {
                ret.add(new Tuple4(rs.getLong("_time"), rs.getDouble("vv"), rs.getDouble("pv"), rs.getLong("uv")));
            }
        }

        assert ret.size() > 0;
    }
}
