package cn.edu.nju.pasa.graph.plangen.incremental;

public class PerformanceEval {
    public static void main(String[] args) throws Exception{
        //long startTime = System.currentTimeMillis();

        //String patternGraphFile = "src/main/java/cn/edu/nju/pasa/graph/plangen/pattern.txt";
        //String dataGraphFile = "src/main/java/cn/edu/nju/pasa/graph/plangen/mygraph";
        //long dataGraphSize = 10000; // the number of vertices in data graph

        String dataGraphFile = args[0];
        long dataGraphSize = Long.parseLong(args[1]);
        String patternGraphFile = args[2];

        IncrementalPlanGen planGenerator = new IncrementalPlanGen(patternGraphFile);
        planGenerator.buildPatternGraph();
        planGenerator.setDataGraphSize(dataGraphSize);
        planGenerator.genDegreeSequence(dataGraphFile);
        planGenerator.calSumOfPower(5);
        planGenerator.generateOptimalPlan();

        //long endTime = System.currentTimeMillis();
        //System.out.println("Total elapsed: " + (endTime - startTime)  + "ms");
    }
}
