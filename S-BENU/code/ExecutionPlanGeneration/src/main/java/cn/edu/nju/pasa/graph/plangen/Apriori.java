package cn.edu.nju.pasa.graph.plangen;

import java.util.*;


/**
 * Apriori algorithm.
 * Created by huweiwei on 6/8/18.
 */
public class Apriori
{
    private List<HashSet<String>> dataList = new ArrayList<HashSet<String>>();
    private int minSupport = 2;

    public Apriori(List<HashSet<String>> dataList, int minSupport) {
        this.dataList = dataList;
        this.minSupport = minSupport;
    }

    public Map<HashSet<String>, Integer> generateAllFrequentSet() {
        Map<HashSet<String>, Integer> frequentSet = new HashMap<HashSet<String>, Integer>();
        List<HashSet<String>> candidates1 = findCadidates1();
        /*
        System.out.println("****************C1****************");
        for(HashSet<String> data: candidates1) {
            for(String item: data) {
                System.out.print(item + " ");
            }
            System.out.println();
        }*/
        Map<HashSet<String>, Integer> tempFrequentSet = findFrequentSet(candidates1);
        frequentSet.putAll(tempFrequentSet);
        /*
        System.out.println("****************L1****************");
        Iterator iter = tempFrequentSet.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            HashSet<String> key = (HashSet<String>)entry.getKey();
            Integer value = (Integer)entry.getValue();
            System.out.println(key + ": " + value);
        }*/

        int k = 2;
        while(!tempFrequentSet.isEmpty()) {
            //System.out.println("****************C" + k + "****************");
            List<HashSet<String>> Ck = aprioriGen(new ArrayList<HashSet<String>>(tempFrequentSet.keySet()));
            /*
            for(HashSet<String> data: Ck) {
                System.out.println(data);
            }*/
            //System.out.println("****************L" + k + "****************");
            tempFrequentSet = findFrequentSet(Ck);
            /*
            Iterator iter2 = tempFrequentSet.entrySet().iterator();
            while(iter2.hasNext()) {
                Map.Entry entry = (Map.Entry) iter2.next();
                HashSet<String> key = (HashSet<String>)entry.getKey();
                Integer value = (Integer)entry.getValue();
                System.out.println(key + ": " + value);
            }*/
            //System.out.println("****************All****************");
            frequentSet.putAll(tempFrequentSet);
            /*
            Iterator iter3 = frequentSet.entrySet().iterator();
            while(iter3.hasNext()) {
                Map.Entry entry = (Map.Entry) iter3.next();
                HashSet<String> key = (HashSet<String>)entry.getKey();
                Integer value = (Integer)entry.getValue();
                System.out.println(key + ": " + value);
            }*/
            k++;
        }
        return frequentSet;
    }

    public Map<HashSet<String>, Integer> generateMaxSizeFrequentSet() {
        Map<HashSet<String>, Integer> maxFrequentSet = new HashMap<>();
        List<HashSet<String>> candidates1 = findCadidates1();

        Map<HashSet<String>, Integer> tempFrequentSet = findFrequentSet(candidates1);

        int k = 2;
        while(!tempFrequentSet.isEmpty()) {
            maxFrequentSet = tempFrequentSet;
            List<HashSet<String>> Ck = aprioriGen(new ArrayList<>(maxFrequentSet.keySet()));
            tempFrequentSet = findFrequentSet(Ck);
            k++;
        }
        return maxFrequentSet;
    }

    /**
     * Lk -> Ck+1, join l1 and l2 for each item in Lk and generate Ck+1.
     */
    private List<HashSet<String>> aprioriGen(List<HashSet<String>> Lk) {
        List<HashSet<String>> Ck = new ArrayList<HashSet<String>>();
        for(int i = 0; i < Lk.size(); i++) {
            HashSet<String> s1 = Lk.get(i);
            int s1Size = s1.size();
            for(int j = i + 1; j < Lk.size(); j++) {
                HashSet<String> s2 = Lk.get(j);
                HashSet<String> sTemp = new HashSet<String>();
                sTemp.addAll(s1);
                sTemp.addAll(s2);
                if(sTemp.size() == s1Size + 1 && !Ck.contains(sTemp)) {
                    //System.out.println("sTemp");
                    //System.out.println(sTemp);
                    Ck.add(sTemp);
                }
            }
        }
        return Ck;
    }

    /**
     * Find C1
     */
    private List<HashSet<String>> findCadidates1() {
        List<HashSet<String>> candidates1 = new ArrayList<HashSet<String>>();
        HashSet<String> itemSet = new HashSet<String>();
        for(HashSet<String> data: dataList) {
            for(String item: data) {
                itemSet.add(item);
            }
        }
        Iterator<String> iter = itemSet.iterator();
        while(iter.hasNext()) {
            HashSet<String> temp = new HashSet<String>();
            temp.add(iter.next());
            candidates1.add(temp);
        }
        return candidates1;
    }

    /**
     * Ck -> Lk, scan dataList, find the candidates who's support greater than min support.
     */
    private Map<HashSet<String>, Integer> findFrequentSet(List<HashSet<String>> candidates) {
        Map<HashSet<String>, Integer> frequentSet = new HashMap<HashSet<String>, Integer>();

        for(HashSet<String> c: candidates) {
            //System.out.print(c);
            int count = 0;
            for(HashSet<String> d: dataList) {
                if(d.containsAll(c)) {
                    count++;
                }
            }
            //System.out.println(": " + count);
            if(count >= minSupport) {
                frequentSet.put(c, count);
            }
        }
        return frequentSet;
    }


}
