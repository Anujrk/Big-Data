import java.sql.*;
public class relations {

   public static void relation() {
       Connection conn = null;
       Statement stmt = null;
       try{
           Class.forName("com.mysql.jdbc.Driver");

           // Open a connection
           System.out.println("Establishing connection to Database");
           conn = DriverManager.getConnection(CreateSources.DB_URL, CreateSources.USER, CreateSources.PASS);
           // Execute a query
           System.out.println("Creating Tables ");
           stmt = conn.createStatement();
           //Create ComedyMovie Table
           String sql = "CREATE TABLE ComedyMovie" +
                   "(id INT ," +
                   "title VARCHAR(100)," +
                   "year INT ,"+
                   "boxoffice VARCHAR(100) ,"+
                   "PRIMARY KEY(id))";
           //Update the table
           stmt.executeUpdate(sql);
           System.out.println("Created ComedyMovie table");

           //Create ActionDramaMovie
          String sql1 = "CREATE TABLE ActionDramaMovie" +
                   "(id INT ," +
                   "title VARCHAR(100)," +
                   "year INT ,"+
                   "color VARCHAR(100),"+
                   "PRIMARY KEY(id))";
           //Update the table
           stmt.executeUpdate(sql1);
           System.out.println("Created ActionDramaMovie table");

           //Create YoungActor
           String sql2 = "CREATE TABLE YoungActor" +
                   "(id INT ," +
                   "name VARCHAR(100)," +
                   "alive TINYINT ,"+
                   "deathcause VARCHAR(100),"+
                   "PRIMARY KEY(id))";
           //Update the table
           stmt.executeUpdate(sql2);
           System.out.println("Created YoungActor table");

           //Create Acted
           String sql3 = "CREATE TABLE Acted" +
                   "(actor INT," +
                   "movie INT," +
                   "PRIMARY KEY(actor,movie))";
           //Update the table
           stmt.executeUpdate(sql3);
           System.out.println("Created Acted table");
           stmt.close();
           conn.close();

       } catch (ClassNotFoundException | SQLException e) {
           e.printStackTrace();
       }
   }
}
