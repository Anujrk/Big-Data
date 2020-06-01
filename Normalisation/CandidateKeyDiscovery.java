import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import sun.util.resources.ext.CalendarData_da;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class CandidateKeyDiscovery {

    static String relation;
    static String fd;
    static String op_name;
    static LinkedHashSet<Set<String>> finalist = new LinkedHashSet<>();

    public static void fd_calculate() {
        relation = relation.replaceAll(" ", "");
        relation = relation.substring(2, relation.length() - 1);
        String[] relation_init = relation.split(",");
        fd = fd.replaceAll(" ", "");
        String[] fd_each = fd.split(";");
        LinkedHashSet<String> left_temp = new LinkedHashSet<>();
        LinkedHashSet<String> right_temp = new LinkedHashSet<>();
        LinkedHashSet<String> left_main = new LinkedHashSet<>();
        LinkedHashSet<String> right_main = new LinkedHashSet<>();
        LinkedHashSet<String> in_both = new LinkedHashSet<>();
        LinkedHashSet<String> total = new LinkedHashSet<>();
        LinkedHashSet<String> notInFd = new LinkedHashSet<>();
        LinkedHashSet<String> core = new LinkedHashSet<>();

        Multimap<Set<String>, Set<String>> relations = ArrayListMultimap.create();
        LinkedHashSet<String> ck = new LinkedHashSet<>();


        for (String temp : fd_each) {
            String trmp = temp.substring(temp.indexOf(">") + 1);
            String[] right_temp2 = trmp.split(",");
            LinkedHashSet<String> right_temp3 = new LinkedHashSet<>(Arrays.asList(right_temp2));
            Collections.addAll(right_temp, right_temp2);
            String trmp2 = temp.substring(0, temp.indexOf("-"));
            String[] left_temp2 = trmp2.split(",");
            LinkedHashSet<String> left_temp3 = new LinkedHashSet<>(Arrays.asList(left_temp2));
            relations.put(left_temp3, right_temp3);
            Collections.addAll(left_temp, left_temp2);
        }
        for (String temp : left_temp) {
            if (!right_temp.contains(temp)) {
                left_main.add(temp);
            } else {
                in_both.add(temp);
            }
        }
        for (String temp : right_temp) {
            if (!left_temp.contains(temp)) {
                right_main.add(temp);
            } else {
                in_both.add(temp);
            }
        }
        total.addAll(left_temp);
        total.addAll(right_temp);

        for (String temp : relation_init) {
            if (!total.contains(temp)) {
                notInFd.add(temp);
            }
        }

        core.addAll(left_main);
        core.addAll(notInFd);





        ck.addAll(core);
        LinkedHashSet<String> past = new LinkedHashSet<>();
        do {
            past.addAll(ck);
            for (Set<String> keys : relations.keySet()) {

                if (ck.containsAll(keys)) {
                    Collection<Set<String>> values = relations.get(keys);
                    for (Set<String> value : values) {
                        ck.addAll(value);
                    }
                }
            }
        }
        while (!past.equals(ck));
        if (ck.containsAll(Arrays.asList(relation_init))) {
            finalist.add(core);


        } else {
            for (int i = 1; i <= in_both.size(); i++) {
                Set<Set<String>> relation_set = Sets.combinations(in_both, i);
                for (Set<String> rel1 : relation_set) {
                    LinkedHashSet<String> addition = new LinkedHashSet<>();
                    addition.addAll(rel1);
                    addition.addAll(core);
                    if (minimalisitc(addition)) {
                        LinkedHashSet<String> ck2 = new LinkedHashSet<>();
                        LinkedHashSet<String> past2 = new LinkedHashSet<>();
                        ck2.addAll(addition);
                        do {
                            past2.addAll(ck2);
                            for (Set<String> keys : relations.keySet()) {
                                if (ck2.containsAll(keys)) {
                                    Collection<Set<String>> values = relations.get(keys);
                                    for (Set<String> value : values) {
                                        ck2.addAll(value);
                                    }
                                }
                            }
                        }
                        while (!past2.equals(ck2));
                        if (ck2.containsAll(Arrays.asList(relation_init))) {
                            finalist.add(addition);

                        }
                    }
                }
            }
        }

        try {
            FileOutputStream outputStream = new FileOutputStream(op_name);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_16);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

            StringBuilder s2 = new StringBuilder();
            for (Set<String> key : finalist) {
                Object[] a = key.toArray();
                String a1 =  a[0].toString();

                StringBuilder s1 = new StringBuilder(a1);
                for (int t = 1; t < a.length; t++) {
                    String add = "," + a[t];
                    s1.append(add);
                }
                s1.append(";");
                s2.append(s1);
            }
            System.out.print(s2.substring(0,s2.length()-1));
            bufferedWriter.write(String.valueOf(s2));




            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean minimalisitc(LinkedHashSet e1){
        for (Set<String> fin:finalist){
            if(e1.containsAll(fin)){
                return false;
            }
        }
        return true;
    }


    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        for (int i = 0; i < args.length; i++)     //taking input for values
        {
           relation = args[0];
           fd = args[1];
            op_name = args[2];
        }
        fd_calculate();
    }
}
