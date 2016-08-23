package com.ydy.phoenixapi;

import java.sql.*;

/**
 * Created by ydy on 2016/6/1.
 */
public class Connector {
    private static Connection conn = null;
    private static Statement stat = null;
    private static ResultSet rs = null;
    private static PreparedStatement ps = null;

    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void closeConn(Connection conn, Statement stat, PreparedStatement ps, ResultSet rs)  {
         try {
             if (rs != null) rs.close();
             if (ps != null) ps.close();
             if (stat != null) stat.close();
             if (conn != null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeConn(Connection conn, Statement stat, ResultSet rs) {
        closeConn(conn, stat, null, rs);
    }

    public static void closeConn(Connection conn,  PreparedStatement ps, ResultSet rs) {
        closeConn(conn, null, ps, rs);
    }

    public static void closeConn(Connection conn, Statement stat) {
        closeConn(conn, stat, null, null);
    }

}

