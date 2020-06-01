package IMDBLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreatePersonTable {
    /*// JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/IMDBLoader";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "Blitzcreek1";*/

    public static void personTable() throws ClassNotFoundException {

        Connection conn = null;
        Statement stmt = null;
        try {

            //Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            // Open a connection
            System.out.println("Establishing connection to Database");
            conn = DriverManager.getConnection(IMDB_Loader.DB_URL, IMDB_Loader.USER, IMDB_Loader.PASS);

            // Execute a query
            System.out.println("Creating Tables ");
            stmt = conn.createStatement();
            //Create Person Table
            String mysql2 = "CREATE TABLE person" +
                    "(id INT PRIMARY KEY," +
                    "name VARCHAR(100)," +
                    "birthYear INT," +
                    "deathYear INT)";
            //Update the table
            stmt.executeUpdate(mysql2);
            System.out.println("Created table Person");
            //   conn.close();
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
