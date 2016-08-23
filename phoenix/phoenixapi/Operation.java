package com.ydy.phoenixapi;

import java.sql.*;

/**
 * Created by ydy on 2016/6/1.
 */
public class Operation {
    private static Connection conn = null;
    private static Statement stat = null;
    private static ResultSet rs = null;
    private static PreparedStatement ps = null;

    public void createTable(String tableName, String rk,String rk_type, String[] cols,String[] col_types)  {
        String sql = "CREATE TABLE \"" + tableName + "\"(\"" + rk_type + "\" " + rk_type + ") NOT NULL PRIMARY KEY,";
        for (int i = 0; i < cols.length; i++) {
            sql += cols[i] + " " + col_types[i] + ",";
        }
        sql += ")";
        try {
            conn = Connector.getConnection();
            stat = conn.createStatement();
            stat.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            Connector.closeConn(conn, stat);
        }
    }

    public void getAllInfo(String tableName) {
        String sql = "SELECT * FROM \"" + tableName + "\"";
        try {
            conn = Connector.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            Connector.closeConn(conn, ps, rs);
        }
    }

    public void upsertTable(String tableName, int id, String col1, String col2) {
        String sql = "UPSERT INTO \"" + tableName + "\"" + "(" + id + "," + col1 + "," + col2 + ")";
        try {
            conn = Connector.getConnection();
            stat = conn.createStatement();
            stat.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            Connector.closeConn(conn, stat);
        }
    }

    public void dropTable(String tableName) {
        String sql = "DROP TABLE \"" + tableName + "\"";
        try {
            conn = Connector.getConnection();
            stat = conn.createStatement();
            stat.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            Connector.closeConn(conn, stat);
        }
    }




}
