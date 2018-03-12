package oracle.demo.oow.bd.dao.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import oracle.demo.oow.bd.constant.Constant;

public class BaseDAO {

    private static Connection conn;
    
    public static Connection getOraConnect() {
        return getOraConnect(Constant.DB_DEMO_USER, Constant.DEMO_PASSWORD);
    }
    
    public static Connection getOraConnect(String user, String password) {

        try {
            Class.forName("com.mysql.jdbc.Driver");
            if (conn == null) {
                conn = DriverManager.getConnection(Constant.JDBC_URL, user, password);
                conn.setAutoCommit(true);
                System.out.println("Connected to database");
            }

        } catch (SQLException se) {
            //se.printStackTrace();
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return conn;
    }
}
