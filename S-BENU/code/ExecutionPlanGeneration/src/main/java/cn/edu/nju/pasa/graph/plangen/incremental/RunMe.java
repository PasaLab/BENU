package cn.edu.nju.pasa.graph.plangen.incremental;

/**
 * Created by huweiwei on 6/24/19.
 */
public class RunMe {

    public static void main(String args[]) throws Exception{
        long startTime = 0;
        long endTime = 0;
        startTime = System.currentTimeMillis();
        /*
        String patternGraphFile = "src/main/java/cn/edu/nju/pasa/graph/plangen/incremental/pattern.txt";
        String dataGraphFile = "src/main/java/cn/edu/nju/pasa/graph/plangen/graph_seed";
        //String dataGraphDegFile = "src/main/java/cn/edu/nju/pasa/graph/plangen/degs_seed";
        long dataGraphSize = 10000; // the number of vertices in data graph

         */
        String dataGraphFile = args[0];
        long dataGraphSize = Long.parseLong(args[1]); // the number of vertices in data graph
        String patternGraphFile = args[2];

        startTime = System.currentTimeMillis();

        IncrementalPlanGen planGenerator = new IncrementalPlanGen(patternGraphFile);

        //1. Build the pattern graph
        planGenerator.buildPatternGraph(); // undirected graph

        //2. Generate degree sequence and sum of power.
        planGenerator.setDataGraphSize(dataGraphSize);
        planGenerator.genDegreeSequence(dataGraphFile);
        planGenerator.calSumOfPower(5);
        endTime = System.currentTimeMillis();
        //System.out.println("Preprocessing elapsed: " + (endTime - startTime)  + "ms");

        // 3. generate optimal execution plan
        planGenerator.generateOptimalPlan();
        endTime = System.currentTimeMillis();
        //System.out.println("Total elapsed: " + (endTime - startTime)  + "ms");
    }
}
