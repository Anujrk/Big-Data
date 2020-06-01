psvmpackage IMDBLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class relations {
    public static void relation() throws ClassNotFoundException {
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
            String sql = "CREATE TABLE hasgenre" +
                    "(movieId INT ," +
                    "genreId  INT ," +
                    "PRIMARY KEY(movieId,genreId)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id)," +
                    "FOREIGN KEY (genreId) REFERENCES Genre(id))";
            //Update the table
            stmt.executeUpdate(sql);
            System.out.println("Created HasGenre table");

            //Create Genre Table
            String sql1 = "CREATE TABLE actedin" +
                    "(personId INT ," +
                    "movieId  INT ," +
                    "PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY (personId) REFERENCES Person(id)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id))";
            //Update the table
            stmt.executeUpdate(sql1);
            System.out.println("Created ActedIn table");

            //Create Genre Table
            String sql2 = "CREATE TABLE composedby" +
                    "(personId INT ," +
                    "movieId  INT ," +
                    "PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY (personId) REFERENCES Person(id)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id))";
            //Update the table
            stmt.executeUpdate(sql2);
            System.out.println("Created ComposedBy table");

            //Create Genre Table
            String sql3 = "CREATE TABLE directedby" +
                    "(personId INT ," +
                    "movieId  INT ," +
                    "PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY (personId) REFERENCES Person(id)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id))";
            //Update the table
            stmt.executeUpdate(sql3);
            System.out.println("Created DirectedBy table");

            //Create Genre Table
            String sql4 = "CREATE TABLE editedby" +
                    "(personId INT ," +
                    "movieId  INT ," +
                    "PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY (personId) REFERENCES Person(id)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id))";
            //Update the table
            stmt.executeUpdate(sql4);
            System.out.println("Created EditedBy table");

            //Create Genre Table
            String sql5 = "CREATE TABLE producedby" +
                    "(personId INT ," +
                    "movieId  INT ," +
                    "PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY (personId) REFERENCES Person(id)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id))";
            //Update the table
            stmt.executeUpdate(sql5);
            System.out.println("Created ProducedBy table");

            //Create Genre Table
            String sql6 = "CREATE TABLE writtenby" +
                    "(personId INT ," +
                    "movieId  INT ," +
                    "PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY (personId) REFERENCES Person(id)," +
                    "FOREIGN KEY (movieId) REFERENCES Movie(id))";
            //Update the table
            stmt.executeUpdate(sql6);
            System.out.println("Created WrittenBy table");
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
