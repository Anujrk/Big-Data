import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.LinkedHashSet;
import java.util.Set;


public class NaiveFDDiscovery {

    static String DB_URL;
    //  Database credentials
    static String USER;
    //= "root";
    static String PASS;
    //= "Blitzcreek1";
    static String[] relation_table;
    static String[] op_name;
    static String[] op_path;
    static Connection conn = null;
    static PreparedStatement pstmt1 = null;
    static LinkedHashSet<Set<String>> candidate = new LinkedHashSet<>();
    static Multimap<String, String> minimal_check = ArrayListMultimap.create();

    public static void naive_relation() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");

        LinkedHashSet<String> pi_A = new LinkedHashSet<>();

        for (int init = 0; init < relation_table.length; init++) {
            String sql = "Select * from " + relation_table[init];
            pstmt1 = conn.prepareStatement(sql);
            ResultSet rs1 = pstmt1.executeQuery();
            ResultSetMetaData rsd = rs1.getMetaData();
            int columnCount = rsd.getColumnCount();
            for (int l = 1; l <= columnCount; l++) {
                String name = rsd.getColumnName(l);
                pi_A.add(name);
            }

            for (int i = 1; i < pi_A.size(); i++) {
                Set<Set<String>> relation_set = Sets.combinations(pi_A, i);
                for (Set obj : relation_set) {
                    Object[] a = obj.toArray();
                    for (String init_set : pi_A) {
                        if (!obj.contains(init_set)) {
                            candidate.add(obj);
                        }
                    }
                }
            }
            for (Set obj : candidate) {
                Object[] a = obj.toArray();
                if (a.length == 1) {
                    for (String init_set : pi_A) {
                        if (!obj.contains(init_set)) {
                            String a1 = (String) a[0];
                            if (!minimal_check.containsKey(init_set) && (!minimal_check.containsValue(a))) {
                                String s1 = "SELECT * FROM " + relation_table[init] + " AS t1 JOIN " + relation_table[init] + " AS t2 ON t1." + a1 + " = t2." + a1 + " WHERE t1."
                                        + init_set + " <> t2." + init_set + " Limit 1;";
                                pstmt1 = conn.prepareStatement(s1);
                                ResultSet rs = pstmt1.executeQuery();
                                int size = 0;
                                while (rs.next()) {
                                    size = rs.getRow();
                                }

                                if (size == 0) {
                                    System.out.println(a1 + " " + init_set);
                                    System.out.println(size);
                                    minimal_check.put(init_set, a1);
                                }
                                rs.close();
                            }
                        }
                    }
                } else {
                    for (String init_set : pi_A) {
                        if (!obj.contains(init_set)) {
                            String a1 = (String) a[0];
                            StringBuilder s1 = new StringBuilder("SELECT * FROM " + relation_table[init] + " AS t1 JOIN " + relation_table[init] + " AS t2 ON t1." + a1 + " = t2." + a1);
                            if (!minimal_check.containsKey(init_set) && (!minimal_check.containsValue(a))) {
                                for (int t = 1; t < a.length; t++) {
                                    String add = " And t1." + a[t] + " = t2." + a[t];
                                    s1.append(add);
                                }
                                String fin = " WHERE t1." + init_set + " <> t2." + init_set + " Limit 1;";
                                s1.append(fin);
                                pstmt1 = conn.prepareStatement(String.valueOf(s1));
                                ResultSet rs = pstmt1.executeQuery();
                                int size = 0;
                                while (rs.next()) {
                                    size = rs.getRow();
                                }

                                if (size == 0) {
                                    System.out.println(a1 + " " + init_set);
                                    System.out.println(size);
                                    for (int m = 0; m < a.length; m++) {
                                        minimal_check.put(init_set, (String) a[m]);
                                    }
                                }
                                rs.close();
                            }
                        }
                    }
                }
            }


            try {
                FileOutputStream outputStream = new FileOutputStream(op_name[init]);
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_16);
                BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

                for (String key : minimal_check.keySet()) {
                    for (String value : minimal_check.get(key)) {
                        bufferedWriter.write(value + "->" + key + "\n");
                        System.out.println(value + "->" + key);
                    }
                }
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        for (int i = 0; i < args.length; i++)     //taking input for values
        {
            DB_URL = args[0];
            USER = args[1];
            PASS = args[2];
            relation_table = args[3].split(",");
            op_name = args[4].split(",");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            naive_relation();
        }
    }
}

