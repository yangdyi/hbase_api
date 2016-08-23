
import java.sql.*;

/**
 * Created by ydy on 16-5-26.
 */
public class Phoenix_hbase_test {
    public static void main(String[] args) throws SQLException {
        Connection conn =null;
        Statement stat = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix");
            stat = conn.createStatement();
            stat.executeUpdate("Create table test_2 (mykey INTEGER NOT NULL PRIMARY KEY ,mycolumn VARCHAR )");
            stat.executeUpdate("Upsert into test_2 values(1,'hello')");
            stat.executeUpdate("Upsert into test_2 values(2,'world')");
            conn.commit();
            ps = conn.prepareStatement("select * from test");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString("mycolumn"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (rs != null) rs.close();
            if (ps != null) ps.close();
            if (stat != null) stat.close();
            if (conn != null) conn.close();
        }
    }
}
