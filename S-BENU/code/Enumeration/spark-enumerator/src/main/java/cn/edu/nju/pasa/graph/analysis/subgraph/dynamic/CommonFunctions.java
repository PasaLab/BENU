package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.util.GenerateBatchUpdateFileFromEdgeList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;

/**
 * Created by bsidb on 12/5/2017.
 */
public class CommonFunctions {

    public static String toStr(int x[]) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < x.length; i++) {
            builder.append(x[i]);
            if (i < x.length - 1)
                builder.append(",");
        }
        builder.append("]");
        return builder.toString();
    }
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

    /**
     * Generate old and new sorted array.
     * All numbers in oldArr is positive, negative and positive numbers are both in deltaArr.
     * Merge old and delta arrays to get newArr. 
     * If a vertex is deleted from oldArr, set the sign bit of the vertex in oldArr to 1.
     * If a vertex is inserted into newArr, set the sign bit of the vertex in newArr to 1.
     * @param oldArr
     * @param deltaArr
     * @return 
     * @throws Exception
     */
    public static int[][] generateOldAndNewSortedArrays(int[] oldArr, int[] deltaArr) throws Exception {
        int[][] oldAndNewArrWithFlag = new int[2][];

        int[] oldArrWithFlag = new int[oldArr.length];
        IntArrayList newArrWithFlag = new IntArrayList();
        int i = 0, j = 0, k = 0;
        while (i < oldArr.length && j < deltaArr.length) {
            int x = oldArr[i];
            int y = setSignBitTo0(deltaArr[j]);
            if(isSignBit1(deltaArr[j])) { // delete vertex
                if(x < y) {
                    oldArrWithFlag[k++] = x;
                    newArrWithFlag.add(x);
                    i++;
                }  else if(x == y) {
                    oldArrWithFlag[k++] = setSignBitTo1(x); // set the sign bit of deleted vertex in oldArr to 1
                    i++; j++;
                } else {
                    // y is not in oldArr
                    throw new Exception("Error: try to delete a nonexistent edge!");
                }
            }  else { // insert vertex
                if (x < y) {
                    oldArrWithFlag[k++] = x;
                    newArrWithFlag.add(x);
                    i++;
                } else if (x > y) {
                    newArrWithFlag.add(setSignBitTo1(y)); // set the sign bit of inserted vertex in newArr to 1
                    j++;
                } else {
                    String oldArrStr = toStr(oldArr);
                    String deltaArrStr = toStr(deltaArr);
                    throw new Exception("Error: try to insert an existent edge! oldArr = " + oldArrStr
                            + ", deltaArr = " + deltaArrStr + ", common vid = " + x);
                }
            }
        }

        while (i < oldArr.length) {
            int x = oldArr[i];
            oldArrWithFlag[k++] = x;
            newArrWithFlag.add(x);
            i++;
        }

        while (j < deltaArr.length) {
            int y = deltaArr[j];
            if(isSignBit1(y)) {
                throw new Exception("Error: try to delete a nonexistent edge!");
            }
            else {
                newArrWithFlag.add(setSignBitTo1(y));
                j++;
            }
        }

        oldAndNewArrWithFlag[0] = oldArrWithFlag;
        oldAndNewArrWithFlag[1] = newArrWithFlag.toIntArray();
        return oldAndNewArrWithFlag;
    }

    /**
     * Merge old and delta sorted array.
     * All numbers in oldArr is positive, negative and positive numbers are both in deltaArr.
     * Delete the number from oldArr if the number is negative in deltaArr.
     * Insert the number into oldArr if the number is positive in deltaArr.
     * The merged result array is sorted.
     * @param oldArr
     * @param deltaArr
     * @return
     */

    public static int[] mergeOldAndDeltaSortedArrays(int[] oldArr, int[] deltaArr) throws Exception {
        IntArrayList sortedArr = new IntArrayList();
        int i = 0, j = 0;
        while (i < oldArr.length && j < deltaArr.length) {
            int x = oldArr[i];
            int y = setSignBitTo0(deltaArr[j]);
            if(isSignBit1(deltaArr[j])) {
                if(x < y) {
                    sortedArr.add(oldArr[i++]);
                } else if(x == y) {
                    i++; j++;
                } else {
                    // y is not in oldArr
                    throw new Exception("Error: try to delete a nonexistent edge!");
                }
            } else {
                if (x < y) {
                    sortedArr.add(oldArr[i++]);
                } else if (x > y) {
                    sortedArr.add(deltaArr[j++]);
                } else {
                    throw new Exception("Error: try to insert an existent edge!");
                }
            }

        }
        while (i < oldArr.length) {
            sortedArr.add(oldArr[i++]);
        }

        while (j < deltaArr.length) {
            if(isSignBit1(deltaArr[j]))
                throw new Exception("Error: try to delete a nonexistent edge!");
            else sortedArr.add(deltaArr[j++]);
        }
        return sortedArr.toIntArray();
    }


    /**
     * Merge two sorted arrays
     * @param arr1
     * @param arr2
     * @return
     */
    public static int[] mergeTwoSortedArrays(int[] arr1, int[] arr2) {
        int[] sortedArr = new int[arr1.length + arr2.length];
        int i = 0, j = 0, k = 0;
        while (i < arr1.length && j < arr2.length) {
            if (arr1[i] < arr2[j]) {
                sortedArr[k++] = arr1[i++];
            } else {
                sortedArr[k++] = arr2[j++];
            }
        }
        while (i < arr1.length) {
            sortedArr[k++] = arr1[i++];
        }

        while (j < arr2.length) {
            sortedArr[k++] = arr2[j++];

        }
        return sortedArr;
    }

    public static int setSignBitTo1(int num) {
        return (num | (1 << 31));
    }

    public static int setSignBitTo0(int num) {
        return (num & (~(1 << 31)));
    }

    public static boolean isSignBit1(int num) {
        return ((num & (1 << 31)) != 0);
    }
}
