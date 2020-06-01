package IMDBLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateMovieTable {

    public static void movieTable() throws ClassNotFoundException {

        Connection conn = null;
        Statement stmt = null;
        try {

            //Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //Open a connection
            System.out.println("Establishing connection to Database");
            conn = DriverManager.getConnection(IMDB_Loader.DB_URL, IMDB_Loader.USER, IMDB_Loader.PASS);

            //Execute a query
            System.out.println("Creating Tables ");
            stmt = conn.createStatement();

            //Create Table for movie
            String mysql1 = "CREATE TABLE movie" +
                    "(id INT PRIMARY KEY," +
                    "title VARCHAR(100)," +
                    "releaseYear INT," +
                    "runtime INT ," +
                    "rating FLOAT," +
                    "numberOfVotes INT)";

            //update movie table
            stmt.executeUpdate(mysql1);
            System.out.println("Created table Movie");
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

