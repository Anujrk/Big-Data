import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class CanonicalCoverComputation {
    static String relation;
    static String fd;
    static String op_name;
    static Multimap<Set<String>, Set<String>> relations = ArrayListMultimap.create();
    static Multimap<Set<String>, Set<String>> mirrorRelations = ArrayListMultimap.create();


    public static void cc_compute() {
        relation = relation.replaceAll(" ", "");
        relation = relation.substring(2, relation.length() - 1);
        String[] relation_init = relation.split(",");
        fd = fd.replaceAll(" ", "");
        String[] fd_each = fd.split(";");

//FOR UNION
        for (String fds : fd_each) {
            String[] left = fds.substring(0, fds.indexOf("-")).split(",");
            LinkedHashSet<String> left1 = new LinkedHashSet<>(Arrays.asList(left));
            String[] right = fds.substring(fds.indexOf(">") + 1).split(",");
            LinkedHashSet<String> right1 = new LinkedHashSet<>(Arrays.asList(right));
            relations.put(left1, right1);
        }

// FOR LEFT SIDE
        LinkedHashSet<Set<String>> toRemove = new LinkedHashSet<>();
        for (Set<String> left : relations.keySet()) {
            for (String left1 : left) {
                Set<String> left2 = Collections.singleton(left1);
                Set<String> remainingLeft = Sets.difference(left, left2);
                LinkedHashSet<String> ck = new LinkedHashSet<>(remainingLeft);
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
                    Collection<Set<String>> right = relations.get(left);
                    toRemove.add(left);
                    for (Set<String> right1 : right) {
                        if (!relations.get(remainingLeft).contains(right1)) {
                            relations.put(remainingLeft, right1);

                        }
                    }

                }
            }

        }
        for (Set<String> removing : toRemove) {
            relations.removeAll(removing);
        }

//FOR RIGHT SIDE
        mirrorRelations = relations;
        for (Set<String> left : relations.keySet()) {
            for (Set<String> right : relations.get(left)) {
                for (String rightValue : right) {
                    Multimap<Set<String>, Set<String>> mirrorRelations2 = ArrayListMultimap.create();
                    Set<String> newRight = Sets.difference(right, Collections.singleton(rightValue));
                    LinkedHashSet<Set<String>> newRight1 = new LinkedHashSet<>(relations.get(left));

                    newRight1.remove(right);

                    newRight1.add(newRight);
                    mirrorRelations2.putAll(left, newRight1);

                    for (Set<String> lefttemp : relations.keySet()) {
                        for (Set<String> righttemp : relations.get(lefttemp)) {
                            if (!mirrorRelations2.containsKey(lefttemp) && !lefttemp.equals(left)) {
                                mirrorRelations2.putAll(lefttemp, relations.get(lefttemp));
                            }
                        }
                    }

                    mirrorRelations = mirrorRelations2;
                    LinkedHashSet<String> ck = new LinkedHashSet<>(left);
                    LinkedHashSet<String> past = new LinkedHashSet<>();
                    do {
                        past.addAll(ck);
                        for (Set<String> keys : mirrorRelations.keySet()) {
                            if (ck.containsAll(keys)) {
                                Collection<Set<String>> values = mirrorRelations.get(keys);
                                for (Set<String> value : values) {
                                    ck.addAll(value);
//                                    System.out.println(mirrorRelations+" - "+rightValue+" = "+ck);
                                }
                            }
                        }
                    }
                    while (!past.equals(ck));
                    if (ck.contains(rightValue)) {
                        relations = mirrorRelations;
                        break;
                    }

                }

            }


        }


        try {
            FileOutputStream outputStream = new FileOutputStream(op_name);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_16);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

            StringBuilder s2 = new StringBuilder();
            for (Set<String> key : relations.keySet()) {
                    Object[] a = key.toArray();
                    String a1 = a[0].toString().replaceAll("\\[","");

                    StringBuilder s1 = new StringBuilder(a1);
                    for (int t = 1; t < a.length; t++) {
                        String add = ", " + a[t].toString().replaceAll("\\[","");;
                        s1.append(add);
                    }
                    String relate = relations.get(key).toString();
                    relate = relate.replaceAll("\\[","");
                relate = relate.replaceAll("\\]","");
                s1.append(" -> ").append(relate).append(";");
                    s2.append(s1);
                }
                String ss =(s2.substring(0, s2.length() - 1));
            System.out.println(ss);
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
            op_name = args[2];
        }
        cc_compute();
    }
}
