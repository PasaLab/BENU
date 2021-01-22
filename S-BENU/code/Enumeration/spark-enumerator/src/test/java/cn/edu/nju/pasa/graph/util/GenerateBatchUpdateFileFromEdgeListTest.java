package cn.edu.nju.pasa.graph.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.Test;

import static org.junit.Assert.*;

public class GenerateBatchUpdateFileFromEdgeListTest {

    public String toStr(IntArrayList x) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < x.size(); i++) {
            builder.append(i);
            if (i < x.size() - 1)
                builder.append(",");
        }
        builder.append("]");
        return builder.toString();
    }
    @Test

    public void convertToIntList() {
        IntArrayList arr = GenerateBatchUpdateFileFromEdgeList.convertToIntList("1 2 3 4");
        int answer[] = {1,2,3,4};
        assertArrayEquals(arr.toIntArray(), answer);
        arr = GenerateBatchUpdateFileFromEdgeList.convertToIntList("100 -2 300 -999");
        int answer2[] = {100, -2, 300, -999};
        assertArrayEquals(arr.toIntArray(), answer2);
    }
}