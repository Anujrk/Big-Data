package DUAQ;

import java.sql.*;

public class DeliberatelyUnsecuredActorQuerying {

    public static void main(String[] args) throws Exception {
        String url = args[0];
        String user = args[1];
        String pwd = args[2];
        String actorName = args[3];

        Connection con = null;
        con = DriverManager.getConnection(url, user, pwd);

        //PreparedStatement takes care of escape characters:
        PreparedStatement st = con.prepareStatement("SELECT DISTINCT id FROM Person AS p JOIN ActedIn as act ON act.personId = p.id where name = (?) LIMIT 1");
        if (actorName==null){
            st.setNull(1, Types.INTEGER);
        }else
        st.setString(1,actorName);

        ResultSet rs = st.executeQuery();
        if (rs.next())
            System.out.println("Id: " + rs.getObject("id"));
        rs.close();
        st.close();
        con.close();
    }
}
