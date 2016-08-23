package com.ydy.phoenixapi;

import java.sql.*;

/**
 * Created by ydy on 2016/6/2.
 *
 * test the performance that  insert into HBase via phoenix
 */
public class Test {
    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Statement stat = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix");
            stat = conn.createStatement();
            long start = System.currentTimeMillis();
            for(int i =0;i<1000000;i++) {
                String sql = "UPSERT INTO \"testydy\" VALUES("+i+",'"+i+"')";
                stat.executeUpdate(sql);
                if (i % 100000 == 0) {
                    conn.commit();
                }
            }
            /*stat.executeUpdate("UPSERT INTO \"testydy\"");
            conn.commit();*/
            long end = System.currentTimeMillis();
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("共用时" + (end - start) / 1000 + "秒");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
           // ps = conn.prepareStatement("EXPLAIN SELECT * FRO \"testydy\"");
           /* rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
           /* if (rs != null) rs.close();
            if (ps != null) ps.close();*/
            if (stat != null) stat.close();
            if (conn != null) conn.close();
        }
    }
}
