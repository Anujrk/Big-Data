import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public class ThreeNFDecomposition {

    static String relation;
    static String fd;
    static String ck;
    static String op_name;
    static Multimap<Set<String>, Set<String>> relations = ArrayListMultimap.create();
    static LinkedHashSet<LinkedHashSet<String>> cks = new LinkedHashSet<>();
    static LinkedHashSet<LinkedHashSet<String>> decompose = new LinkedHashSet<>();


    public static void nfD() {
        relation = relation.replaceAll(" ", "");
        relation = relation.substring(2, relation.length() - 1);
        String[] relation_init = relation.split(",");
        fd = fd.replaceAll(" ", "");
        String[] fd_each = fd.split(";");


        for (String fds : fd_each) {
            String[] left = fds.substring(0, fds.indexOf("-")).split(",");
            LinkedHashSet<String> left1 = new LinkedHashSet<>(Arrays.asList(left));
            String[] right = fds.substring(fds.indexOf(">") + 1).split(",");
            LinkedHashSet<String> right1 = new LinkedHashSet<>(Arrays.asList(right));
            relations.put(left1, right1);
        }

        String[] insidecktemp = ck.split(";");
        for (String temp2 : insidecktemp) {
            String[] insidetemp2 = temp2.split(",");
            LinkedHashSet<String> inside = new LinkedHashSet<>();
            inside.addAll(Arrays.asList(insidetemp2));
            cks.add(inside);
        }

        //Checking for 3Conditions:
        int counter = 0;
        for (Set<String> keys : relations.keySet()) {
            Collection<Set<String>> values = relations.get(keys);
            for (String key : keys) {
                for (Set<String> val : values) {
                    if (val.contains(key)) {       //Checking trivial
                        break;
                    } else {
//                        System.out.println(key + "is not trivial");
                        LinkedHashSet<String> ck = new LinkedHashSet<>();
                        ck.addAll(keys);
                        LinkedHashSet<String> past = new LinkedHashSet<>();
                        do {
                            past.addAll(ck);
                            for (Set<String> keys1 : relations.keySet()) {
                                if (ck.containsAll(keys1)) {
                                    Collection<Set<String>> values1 = relations.get(keys1);
                                    for (Set<String> value : values1) {
                                        ck.addAll(value);
                                    }
                                }
                            }
                        }
                        while (!past.equals(ck));
                        if (ck.containsAll(Arrays.asList(relation_init))) {
                            break;
                        } else {
                            for (Set<String> cond3 : cks) {
                                Collection<Set<String>> values2 = relations.get(keys);
                                for (Set<String> check : values2) {
                                    if (cond3.containsAll(check)) {
                                        break;
                                    } else {
                                        if(counter==0){
                                        decompose();}
                                        counter++;
                                    }
                                }
                            }
                        }
                    }


                }
            }
        }
    }
    public static void decompose() {

        for(Set<String> key:relations.keySet()){
            LinkedHashSet<String> relation2 = new LinkedHashSet<>();
            for(Set<String> value:relations.get(key)) {
                relation2.addAll(value);
                relation2.addAll(key);
            }
            decompose.add(relation2);
        }
        LinkedHashSet<String> temp = new LinkedHashSet<>();
        int counter = 0;
        for(Set<String> contains:decompose){
            for(Set<String>keys :cks){
                if(contains.containsAll(keys)){
                    counter ++;
                }else{
                    for(LinkedHashSet<String> t :cks){
                        temp.addAll(t);
                        break;
                    }
                }
            }
        }
        if(counter==0) {
            decompose.add(temp);
        }
        LinkedHashSet<String> finals = new LinkedHashSet<>();
        for(Set<String> red1:decompose){
            for(Set<String> red2:decompose){
                if(red1.containsAll(red2)){
                    for(Set<String> add:decompose){
                        if(add!=red1){
                            finals.add(String.valueOf(add));
                        }
                    }
                }
            }
        }

        try {
            FileOutputStream outputStream = new FileOutputStream(op_name);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_16);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

            StringBuilder s1 = new StringBuilder();
            int i = 0;
            for(Set<String> print:decompose){
                s1.append("r"+i+print);
                s1.append(";");
                i++;
            }

            String ss = s1.toString().substring(0,s1.length()-1);
            ss = ss.replaceAll("\\[","\\(");
            ss = ss.replaceAll("\\]","\\)");
            bufferedWriter.write(ss);
            bufferedWriter.close();
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++)     //taking input for values
        {
            relation = args[0];
            fd = args[1];
            ck = args[2];
            op_name = args[3];

        }
        nfD();
    }
}

