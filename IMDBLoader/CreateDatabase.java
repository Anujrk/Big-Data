package IMDBLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateDatabase {


    public static void dataBase() {
        final String DB_URL = "jdbc:mysql://localhost/";
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Establishing connection to DataBase");
            conn = DriverManager.getConnection(DB_URL, IMDB_Loader.USER, IMDB_Loader.PASS);
            System.out.println("Creating Database");
            stmt = conn.createStatement();
            String sql = "CREATE DATABASE IMDBLoader";
            stmt.executeUpdate(sql);
            System.out.println("Database Created");
            conn.close();
            stmt.close();

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
