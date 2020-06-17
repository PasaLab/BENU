package cn.edu.nju.pasa.graph.plangen.incremental;

import java.io.File;

public class PruningEval {
    public static void main(String[] args) throws Exception{
        long startTime = 0;
        long endTime = 0;

        double totalTime = 0;

        double totalCostEstimateTimes = 0;
        double totalOptimalDBMatchingOrder = 0;

        //String patternGraphFilePath = "src/main/java/cn/edu/nju/pasa/graph/plangen/graph10c";
        //String dataGraphFile = "src/main/java/cn/edu/nju/pasa/graph/plangen/mygraph";
        //long dataGraphSize = 10000;

        String dataGraphFile = args[0];
        long dataGraphSize = Long.parseLong(args[1]);
        String patternGraphFilePath = args[2];

        File inputFile = new File(patternGraphFilePath);
        int incrementalPatternGraphNum = 0;
        if (inputFile.exists()) {
            if(inputFile.isDirectory()) { // random graphs
                String[] files = inputFile.list();
                for (int i = 0; i < files.length; i++) {
                    startTime = System.currentTimeMillis();
                    System.out.println("**************************** file: " + files[i] + "****************************");
                    IncrementalPlanGen planGenerator = new IncrementalPlanGen(patternGraphFilePath + "/" + files[i]);
                    planGenerator.buildPatternGraph();
                    planGenerator.setDataGraphSize(dataGraphSize);
                    planGenerator.genDegreeSequence(dataGraphFile);
                    planGenerator.calSumOfPower(5);
                    planGenerator.generateOptimalPlan();

                    totalOptimalDBMatchingOrder += planGenerator.getOptimizedDBMatchingOrderSize();
                    totalCostEstimateTimes += planGenerator.getCostEstimateCounter();
                    incrementalPatternGraphNum += planGenerator.getPatternGraphsEdgesNum();

                    endTime = System.currentTimeMillis();
                    totalTime = totalTime + (endTime - startTime);
                    System.out.println("Total elapsed: " + (endTime - startTime) + "ms");
                }
                System.out.println("File num: " + files.length);
                System.out.println("incrementalPatternGraphNum: " + incrementalPatternGraphNum);
                //float fileLength = (float) files.length;
                System.out.println("Average cost estimate times: " + (totalCostEstimateTimes / incrementalPatternGraphNum));
                System.out.println("Average optimal DB matching order size: " + (totalOptimalDBMatchingOrder / incrementalPatternGraphNum));
                System.out.println("Average execution plan generation time: " + (totalTime / incrementalPatternGraphNum) + "ms");
            } else { // paper graph
                startTime = System.currentTimeMillis();
                IncrementalPlanGen planGenerator = new IncrementalPlanGen(patternGraphFilePath);
                planGenerator.buildPatternGraph();
                planGenerator.setDataGraphSize(dataGraphSize);
                planGenerator.genDegreeSequence(dataGraphFile);
                planGenerator.calSumOfPower(5);
                planGenerator.generateOptimalPlan();

                totalOptimalDBMatchingOrder += planGenerator.getOptimizedDBMatchingOrderSize();
                totalCostEstimateTimes += planGenerator.getCostEstimateCounter();
                incrementalPatternGraphNum += planGenerator.getPatternGraphsEdgesNum();

                endTime = System.currentTimeMillis();
                totalTime = totalTime + (endTime - startTime);
                System.out.println("Total elapsed: " + (endTime - startTime) + "ms");

                System.out.println("File num: " + 1);
                System.out.println("incrementalPatternGraphNum: " + incrementalPatternGraphNum);
                //float fileLength = (float) files.length;
                System.out.println("Average cost estimate times: " + (totalCostEstimateTimes / incrementalPatternGraphNum));
                System.out.println("Average optimal DB matching order size: " + (totalOptimalDBMatchingOrder / incrementalPatternGraphNum));
                System.out.println("Average execution plan generation time: " + (totalTime / incrementalPatternGraphNum) + "ms");
            }
        }
    }
}
