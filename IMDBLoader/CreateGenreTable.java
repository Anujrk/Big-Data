package IMDBLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateGenreTable {
    public static void genreTable() {

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

            //Create Genre Table
            String mysql3 = "CREATE TABLE genre" +
                    "(id INT NOT NULL AUTO_INCREMENT," +
                    "name VARCHAR(100)," +
                    "PRIMARY KEY(id))";

            //Update the table
            stmt.executeUpdate(mysql3);
            System.out.println("Created table genre");
            //      conn.close();
            stmt.close();

        } catch (SQLException | ClassNotFoundException e) {
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

