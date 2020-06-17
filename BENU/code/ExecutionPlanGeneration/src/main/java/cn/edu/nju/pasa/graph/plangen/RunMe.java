package cn.edu.nju.pasa.graph.plangen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by huweiwei on 5/25/18.
 */

class ConstantCharacter {
    public static final boolean DEBUG = false;
    public static final boolean DEGREEFILTER = false;
    public static final boolean COMPRESS = true;
    public static final String DATAGRAPH = " ";
    public static final String ENUTARGET = "f";
    public static final String DBQTARGET = "A";
    public static final String INTTARGET = "T";
    public static final String INTCANDIDATE = "C";
    public static final String DATAVERTEXSET = "V";
}

public class RunMe {

    public static void main(String args[]) throws Exception{
        long startTime = 0;
        long endTime = 0;
        startTime = System.currentTimeMillis();

        String dataGraphFile = args[0];
        long dataGraphSize = Long.parseLong(args[1]); // the number of vertices in data graph
        String patternGraphFile = args[2];

        startTime = System.currentTimeMillis();

        PlanGen planGenerator = new PlanGen(patternGraphFile);

        //1. Build the pattern graph
        planGenerator.buildPatternGraph(); // undirected graph

        //2. Generate degree sequence and sum of power.
        //   Each line of dataGraphFile is vertex and the adjacent vertices of it separated with " ".
        planGenerator.setDataGraphSize(dataGraphSize);
        planGenerator.genDegreeSequence(dataGraphFile);
        planGenerator.calSumOfPower(5);
        endTime = System.currentTimeMillis();
        System.out.println("Preprocessing elapsed: " + (endTime - startTime)  + "ms");

        // 3. generate optimal execution plan
        planGenerator.generateOptimalPlan();
        endTime = System.currentTimeMillis();
        System.out.println("Total elapsed: " + (endTime - startTime)  + "ms");
    }
}
