package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;

/**
 * Created by bsidb on 12/5/2017.
 */
public class CommonFunctions {
     /**
     * Find vertices greater than vid
     *
     * @param resultList the result is stored in this list
     * @param adj        the adj list
     * @param vid
     */
    public static void filterAdjList(IntArrayList resultList, int[] adj, int vid) {
        resultList.clear();
        for (int i = 0; i < adj.length; i++) {
            if (adj[i] > vid) {
                resultList.add(adj[i]);
            }
        }
    }

    public static void filterAdjListAdaptive(IntArrayList resultList, int[] adj, int vid) {
        resultList.clear();
        int pos = Arrays.binarySearch(adj, vid + 1);
        if (pos < 0) pos = -pos - 1;
        for (int i = pos; i < adj.length; i++) {
            resultList.add(adj[i]);
        }
    }


    public static void filterAdjListAdaptive(IntArrayList resultList, IntArrayList adjList, int vid) {
        int adj[] = adjList.toIntArray();
        filterAdjListAdaptive(resultList, adj, vid);
    }

    public static void filterAdjListAdaptive(IntArrayList resultList, int[] adj, int vid, Int2IntMap degreeMap, int degree) {
        resultList.clear();
        int pos = Arrays.binarySearch(adj, vid + 1);
        if (pos < 0) pos = -pos - 1;
        for (int i = pos; i < adj.length; i++) {
            if (degreeMap.getOrDefault(adj[i], 0) >= degree) {
                resultList.addElements(0, adj, i, adj.length - i);
                break;
            }
        }
    }

    public static void filterAdjListAdaptive(IntArrayList resultList, IntArrayList adjList, int vid, Int2IntMap degreeMap, int degree) {
        int adj[] = adjList.toIntArray();
        filterAdjListAdaptive(resultList, adj, vid, degreeMap, degree);
    }

    public static void filterInjectiveConds(IntArrayList resultList, IntArrayList adjList, int vid) {
        /*
        resultList.clear();
        resultList.addAll(adjList);
        int adj[] = adjList.toIntArray();
        int pos = Arrays.binarySearch(adj, vid);
        if (pos > 0) {
            resultList.removeInt(pos);
        }*/

        resultList.clear();
        int adj[] = adjList.toIntArray();
        int pos = Arrays.binarySearch(adj, vid);
        if (pos > 0) {
            resultList.addElements(0, adj, 0, pos);
            resultList.addElements(pos, adj, pos+1, adj.length - pos - 1);
        } else {
            resultList.addElements(0, adj, 0, adj.length);
        }
    }

    public static void filterAdjList(IntArrayList resultList, IntArrayList a, int vid) {
        resultList.clear();
        int[] aArray = a.toIntArray();
        for (int i = 0; i < aArray.length; i++) {
            if (aArray[i] > vid) {
                resultList.add(aArray[i]);
            }
        }
    }

    /**
     * Find vertices who's degree in data graph greater than degree
     *
     * @param resultList the result is stored in this list
     * @param a          the vertices list
     * @param degreeMap  the degree map of vertices
     * @param degree
     */
    public static void filterAdjList(IntArrayList resultList, IntArrayList a, Int2IntMap degreeMap, int degree) {
        resultList.clear();
        int[] aArray = a.toIntArray();
        for (int i = 0; i < aArray.length; i++) {
            if (degreeMap.getOrDefault(aArray[i], 0) >= degree) {
                resultList.add(aArray[i]);
            }
        }
    }

    public static void filterAdjList(IntArrayList resultList, int[] adj, Int2IntMap degreeMap, int degree) {
        resultList.clear();
        for (int i = 0; i < adj.length; i++) {
            if (degreeMap.getOrDefault(adj[i], 0) >= degree) {
                //resultList.add(adj[i]);
                resultList.addElements(0, adj, i, adj.length - i);
                break;
            }
        }
    }

    public static void filterAdjList(IntArrayList resultList, int[] adj, int vid, Int2IntMap degreeMap, int degree) {
        resultList.clear();
        for (int i = 0; i < adj.length; i++) {
            int v = adj[i];
            if (v > vid && degreeMap.getOrDefault(v, 0) >= degree) {
                resultList.add(adj[i]);
            }
        }
    }

    public static void filterAdjList(IntArrayList resultList, IntArrayList a, int vid, Int2IntMap degreeMap, int degree) {
        resultList.clear();
        int[] aArray = a.toIntArray();
        for (int i = 0; i < aArray.length; i++) {
            int v = aArray[i];
            if (v > vid && degreeMap.getOrDefault(v, 0) >= degree) {
                resultList.add(aArray[i]);
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, IntArrayList a1, IntArrayList a2) {
        result.clear();
        int[] a1Array = a1.toIntArray();
        int[] a2Array = a2.toIntArray();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                result.add(a1Array[i]);
                i++; j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, int[] a1Array, int[] a2Array) {
        result.clear();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, IntArrayList a1, int[] a2Array) {
        result.clear();
        int[] a1Array = a1.toIntArray();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, int[] a1Array, int[] a2Array, int greaterThan) {
        result.clear();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                if (a1Array[i] > greaterThan)
                    result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, IntArrayList a1, int[] a2Array, int greaterThan) {
        result.clear();
        int[] a1Array = a1.toIntArray();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                if (a1Array[i] > greaterThan)
                    result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, IntArrayList a1, IntArrayList a2, int greaterThan) {
        result.clear();
        int[] a1Array = a1.toIntArray();
        int[] a2Array = a2.toIntArray();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                if (a1Array[i] > greaterThan)
                    result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectTwoSortedArray(IntArrayList result, int[] a1Array, int[] a2Array, int greaterThan, Int2IntMap degreeMap, int degree) {
        result.clear();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                if (a1Array[i] > greaterThan && degreeMap.get(a1Array[i]) >= degree)
                    result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }


    public static void intersectTwoSortedArray(IntArrayList result, IntArrayList a1, int[] a2Array, int greaterThan, Int2IntMap degreeMap, int degree) {
        result.clear();
        int[] a1Array = a1.toIntArray();
        int i = 0, j = 0;
        while (i < a1Array.length && j < a2Array.length) {
            if (a1Array[i] == a2Array[j]) {
                if (a1Array[i] > greaterThan && degreeMap.get(a1Array[i]) >= degree)
                    result.add(a1Array[i]);
                i++;
                j++;
            } else if (a1Array[i] < a2Array[j]) {
                i++;
            } else {
                j++;
            }
        }
    }

    public static void intersectThreeSortedArray(IntArrayList result, int[] a1Array, int[] a2Array, int[] a3Array) {
        result.clear();
        int a = 0, b = 0, c = 0;
        int i = 0, j = 0, k = 0;
        while (a < a1Array.length && b < a2Array.length && c < a3Array.length) {
            i = a; j = b; k = c;
            if ((a1Array[i] == a2Array[j]) && (a2Array[j] == a3Array[k])) {
                result.add(a1Array[i]);
                a++;
                b++;
                c++;
            } else {
                if ((a2Array[j] > a1Array[i]) || (a3Array[k] > a1Array[i])) {
                    a++;
                }
                if ((a1Array[i] > a2Array[j]) || (a3Array[k] > a2Array[j])) {
                    b++;
                }
                if ((a1Array[i] > a3Array[k]) || (a2Array[j] > a3Array[k])) {
                    c++;
                }
            }
        }
    }

    public final static void intersectTwoSortedArrayAdaptive(IntArrayList result, int a1Array[], int a2Array[]) {
        result.clear();
        if (a1Array.length > a2Array.length) {
            int[] t = a1Array;
            a1Array = a2Array;
            a2Array = t;
        }

        if (a1Array.length * Math.log(a2Array.length)/Math.log(2.0) < a2Array.length) {
            // It is more efficient to use binary search intersect
            for(int i = 0; i < a1Array.length; i++) {
                int x = a1Array[i];
                int posY = Arrays.binarySearch(a2Array, x);
                if (posY >= 0) result.add(x);
            }
        } else {
            int i = 0, j = 0;
            while (i < a1Array.length && j < a2Array.length) {
                if (a1Array[i] == a2Array[j]) {
                    result.add(a1Array[i]);
                    i++;
                    j++;
                } else if (a1Array[i] < a2Array[j]) {
                    i++;
                } else {
                    j++;
                }
            }
        }
    }

    public static void intersectTwoSortedArrayAdaptive(IntArrayList result, IntArrayList a1, int[] a2Array) {
        int[] a1Array = a1.toIntArray();
        intersectTwoSortedArrayAdaptive(result, a1Array, a2Array);
    }

    public static void intersectTwoSortedArrayAdaptive(IntArrayList result, IntArrayList a1, IntArrayList a2) {
        intersectTwoSortedArrayAdaptive(result, a1.toIntArray(), a2.toIntArray());
    }

    public static void intersectThreeSortedArray(IntArrayList result, int[] a1Array, int[] a2Array, int[] a3Array, int greaterThan) {
        result.clear();
        int a = 0, b = 0, c = 0;
        int i = 0, j = 0, k = 0;
        while (a < a1Array.length && b < a2Array.length && c < a3Array.length) {
            i = a; j = b; k = c;
            if ((a1Array[i] == a2Array[j]) && (a2Array[j] == a3Array[k])) {
                if(a1Array[i] > greaterThan) result.add(a1Array[i]);
                a++;
                b++;
                c++;
            } else {
                if ((a2Array[j] > a1Array[i]) || (a3Array[k] > a1Array[i])) {
                    a++;
                }
                if ((a1Array[i] > a2Array[j]) || (a3Array[k] > a2Array[j])) {
                    b++;
                }
                if ((a1Array[i] > a3Array[k]) || (a2Array[j] > a3Array[k])) {
                    c++;
                }
            }
        }
    }


}
