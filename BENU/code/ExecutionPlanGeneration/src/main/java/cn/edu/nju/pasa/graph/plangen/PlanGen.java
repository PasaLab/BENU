package cn.edu.nju.pasa.graph.plangen;

import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by huweiwei on 5/23/18.
 */
public class PlanGen {
    private String patternGraphFile = "";
    private Graph pattern = new Graph();
    private Map<Integer, List<Integer>> symmetryBreakingConditions = new HashMap<>();
    // v: automorphism symmetry-breaking condition eg.u0: <u1,<u2,>u3,>u4 stored as 0: -1,-2,+3,+4
    private ArrayList<Integer> degArray = new ArrayList<>();
    //private HashMap<Integer, Double> sumOfPower = new HashMap<>();
    private HashMap<Integer, BigDecimal> sumOfPower = new HashMap<>();
    private long dataGraphVertexNum = 0;
    private int Ti = 0;
    //private long optimalDBCost = Long.MAX_VALUE;
    private BigInteger optimalDBCost = BigInteger.valueOf(-1);
    private List<List<Integer>> optimizedDBMatchingOrder = new ArrayList<>();
    // store all the optimal matching order optimized db costs. each element is like <u1, u2, u3, ...>
    private List<List<ExecutionInstruction>> optimizedComputationalExecPlan = new ArrayList<>();
    // store all the execution plan optimized computational costs

    private int costEstimateCounter = 0;

    public PlanGen(String patternGraphFile) {
        this.patternGraphFile = patternGraphFile;
    }

    public PlanGen(Graph pattern) {
        this.pattern = pattern;
    }

    public int getCostEstimateCounter() {return costEstimateCounter;}

    public int getOptimizedDBMatchingOrderSize() {return optimizedDBMatchingOrder.size();}

    public void generateOptimalPlan() {
        long startTime = System.currentTimeMillis();

        List<Integer> vertexList = new ArrayList<>(pattern.getAllVertices());
        Collections.sort(vertexList);

        // 0. calculate equivalent classes of pattern vertexes
        List<List<Integer>> equivalentVertex = new ArrayList<>();
        for(int v1: vertexList) {
            boolean flag = true;
            if (!equivalentVertex.isEmpty()) {
                for (List<Integer> s : equivalentVertex) {
                    int v2 = s.get(0);
                    List<Integer> adj1 = new ArrayList<>(pattern.getAdj(v1));
                    List<Integer> adj2 = new ArrayList<>(pattern.getAdj(v2));
                    adj1.remove(new Integer(v2));
                    adj2.remove(new Integer(v1));
                    if (adj1.size() == adj2.size() && adj1.containsAll(adj2)) {
                        s.add(v1);
                        flag = false;
                        break;
                    }
                }
            }
            if (flag) { // equivalentVertex is empty or there is no veretx set equivalent to v1
                List<Integer> s = new ArrayList<>();
                s.add(v1);
                equivalentVertex.add(s);
            }
        }

        if(ConstantCharacter.DEBUG) {
            System.out.println("equivalent vertex set");
            for (List<Integer> s : equivalentVertex) {
                for (int v : s) {
                    System.out.print(v + " ");
                }
                System.out.println();
            }
        }

        // 1. enumerate every possible connected permulation of the pattern vertexes, and retain matching orders with optimal db costs
        int[] matchedAdj = new int[pattern.getVertexNum()];
        // each element stores the number of matched neighbors of vertex. For matched vertex, set the element negative number
        //the position of each vertex in the list is corresponding to the position in matchedAdj

        List<Integer> matchedVertex = new ArrayList<>();
        Graph partialPattern = new Graph();
        //List<Long> estimatedCost = new ArrayList<>();
        List<BigInteger> estimatedCost = new ArrayList<>();
        // each cost is corresponding to the sum of previous and current matched vertex's db operation cost.

        for (int i = 0; i < vertexList.size(); i++) {
            matchedVertex(vertexList.get(i), i, matchedAdj, vertexList, matchedVertex, partialPattern, estimatedCost,
                    equivalentVertex);
            //matchedAdj[i] = 0;
        }

        if(ConstantCharacter.DEBUG ) {
            System.out.println("Optimized DBCost Matching Order: ");
            for(List<Integer> orders: optimizedDBMatchingOrder) {
                for(int o: orders) {
                    System.out.print(o + " ");
                }
                System.out.println();
            }
        }

        System.out.println("optimizedDBMatchingOrder.size: " + optimizedDBMatchingOrder.size());
        System.out.println("estimate partial pattern times: " + costEstimateCounter);

        long endTime = System.currentTimeMillis();
        System.out.println("Generate all matching orders with db optimized elapsed: " + (endTime - startTime)  + "ms");


        startTime = System.currentTimeMillis();

        // for all matching orders with optimized db costs
        for(List<Integer> orders: optimizedDBMatchingOrder) {
            // 2. generate raw execution plan
            List<ExecutionInstruction> execPlan = generateRawPlanFromMatchingOrder(orders);
            if(ConstantCharacter.DEBUG) {
                System.out.print("matching order: ");
                for(int o: orders) {
                    System.out.print(o + " ");
                }
                System.out.println();
                System.out.println("----------raw execution plan---------------");
                for (ExecutionInstruction instruction : execPlan) {
                    System.out.println(instruction);
                }
            }

            // 3. optimize computational cost

            // optimization 1: common subexpression elimination
            eliminateCommonSubExpression(execPlan);
            // optimization 2: instruction reordering
            flatten(execPlan);
            reorder(execPlan);
            // optimization 3: triangle caching
            cacheTriangle(execPlan);

            addFilteringCondition(execPlan);
            optimizedComputationalExecPlan.add(execPlan);

        }

        // 4. select the optimal execution plan with least db costs and computational costs
        selectOptimalExecPlan();

        endTime = System.currentTimeMillis();
        System.out.println("Computational cost optimized elapsed: " + (endTime - startTime)  + "ms");
    }

    private void matchedVertex(int vid, int index, int[] matchedAdj, List<Integer> vertexList, List<Integer> matchedVertex,
                               Graph partialPattern, List<BigInteger> estimatedCost,
                               List<List<Integer>> equivalentVertex) {

        // check the matching order of equivalent vertices
        for(List<Integer> s: equivalentVertex){
            if(s.contains(vid)) {
                for(int v: s) { // s has been sorted
                    if(v >= vid) break; // all smaller equivalent vertices has been matched
                    else if(!matchedVertex.contains(v)) {
                        //System.out.println("break the order: " + vid + " before " + v);
                        return; // break the matching order of equivalent vertices
                    }
                }
                break;
            }
        }

        // match vid
        matchedVertex.add(vid);

        if (matchedVertex.size() >= pattern.getVertexNum()) {
            // all pattern vertices have been matched
            /*
            if(ConstantCharacter.DEBUG) {
                for (int x : matchedVertex) System.out.print(x + " ");
                System.out.println("matching order");
                System.out.println("cost: ");
                //for (long c : estimatedCost) System.out.print(c + " ");
                for (BigInteger c : estimatedCost) System.out.print(c + " ");
                System.out.println("totalcost: " + estimatedCost.get(estimatedCost.size() - 1));
            }
            */
            //long totalCost = estimatedCost.get(estimatedCost.size() - 1);
            BigInteger totalCost = estimatedCost.get(estimatedCost.size() - 1);
            //if(totalCost < optimalDBCost) {
            if(totalCost.compareTo(optimalDBCost) < 0 || optimalDBCost.compareTo(BigInteger.ZERO) < 0) {
                optimizedDBMatchingOrder.clear();
            }
            optimalDBCost = totalCost;
            optimizedDBMatchingOrder.add(new ArrayList<>(matchedVertex));
            //ArrayList<Long> dBEstimateCostList = new ArrayList<>(estimatedCost);
            ArrayList<BigInteger> dBEstimateCostList = new ArrayList<>(estimatedCost);
            dBEstimateCostList.add(totalCost);
            matchedVertex.remove(matchedVertex.size() - 1);
            return;
        } else if(matchedVertex.size() >= 2){

            updatePartialPattern(vid, matchedVertex, partialPattern);


            //long totalCost;
            BigInteger totalCost;
            if(matchedAdj[index] < pattern.getDegree(vid)) { // Case 1
                // estimate database cost
                PartialPatternMatchesEstimator estimator = new PartialPatternMatchesEstimator(partialPattern);
                //long cost = estimator.computeCost(degArray, sumOfPower, matchedVertex.get(0));
                BigInteger cost = estimator.computeCost(degArray, sumOfPower, matchedVertex.get(0), dataGraphVertexNum);

                costEstimateCounter++;

                //totalCost = estimatedCost.get(estimatedCost.size() - 1) + cost;
                totalCost = estimatedCost.get(estimatedCost.size() - 1).add(cost);

                //if(totalCost > optimalDBCost) {

                if(totalCost.compareTo(optimalDBCost) > 0 && optimalDBCost.compareTo(BigInteger.valueOf(-1)) != 0) {
                    matchedVertex.remove(matchedVertex.size() - 1);
                    //System.out.println("filter");
                    //System.out.println("cost: " + totalCost);

                    partialPattern.removeVertex(vid);
                    return;
                }

            } else { // Case 2
                // the db operation is useless and will never be used, the cost doesn't increase, add directly.
                totalCost = estimatedCost.get(estimatedCost.size() - 1);
            }
            estimatedCost.add(totalCost);

        } else { // vid is the first matched vertex.
            //matchedAdj[index] = Integer.MIN_VALUE;
            estimatedCost.add(BigInteger.valueOf(dataGraphVertexNum));
            partialPattern.addVertex(vid);
            costEstimateCounter++;
        }

        // turn the number of matched neighborhoods of vid negative
        matchedAdj[index] = -matchedAdj[index];
        List<Integer> adjList = pattern.getAdj(vid);
        // update matchedAdj
        for (int j = 0; j < vertexList.size(); j++) {
            //if (matchedAdj[j] >= 0 && adjList.contains(vertexList.get(j))) {
            int v = vertexList.get(j);
            if (!matchedVertex.contains(v) && adjList.contains(v)) {
                matchedAdj[j] += 1;
            }
        }
        /*
        System.out.println("matchedVertex");
        for(int x: matchedVertex) {
            System.out.print(x + " ");
        }
        System.out.println();
        System.out.println("vertexList");
        for(int x: vertexList) {
            System.out.print(x + " ");
        }
        System.out.println();
        System.out.println("matchedAdj");
        for(int x: matchedAdj) {
            System.out.print(x + " ");
        }
        System.out.println();
        */
        // for disconnected partial pattern graph
        for (int j = 0; j < vertexList.size(); j++) {
            if (!matchedVertex.contains(vertexList.get(j))) {
                matchedVertex(vertexList.get(j), j, matchedAdj, vertexList, matchedVertex, partialPattern, estimatedCost,
                        equivalentVertex);
            }
        }

        // for connected partial pattern graph
        /*
        for (int j = 0; j < matchedAdj.length; j++) {
            if (matchedAdj[j] > 0) {
                matchedVertex(vertexList.get(j), j, matchedAdj, vertexList, matchedVertex, partialPattern, estimatedCost,
                        equivalentVertex);
            }
        }
        */

        matchedAdj[index] = -matchedAdj[index];
        matchedVertex.remove(matchedVertex.size() - 1);
        for (int j = 0; j < vertexList.size(); j++) {
            int v = vertexList.get(j);
            if (!matchedVertex.contains(v) && adjList.contains(v)) {
                matchedAdj[j] -= 1;
            }
        }
        estimatedCost.remove(estimatedCost.size() - 1);

        partialPattern.removeVertex(vid);
    }

    private void updatePartialPattern(int vid, List<Integer> matchedVertex, Graph partialPattern) {
        // add edges between the last matched vertex and all its neighbors who have been matched.
        partialPattern.addVertex(vid);
        for(int x: matchedVertex) {
            if(pattern.getAdj(vid).contains(x)) {
                partialPattern.addEdge(x, vid);
            }
        }
    }

    private List<ExecutionInstruction> generateRawPlanFromMatchingOrder(List<Integer> orders) {
        List<ExecutionInstruction> execPlan = new ArrayList<>();
        Set<Integer> fSet = new HashSet<Integer>();

        // generate instructions for the first matching vertex
        int id = orders.get(0);
        execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INI,
                ConstantCharacter.ENUTARGET + id, ""));
        execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                ConstantCharacter.DBQTARGET + id, ConstantCharacter.ENUTARGET + id));
        fSet.add(id);

        for(int i = 1; i < orders.size(); i++) {
            id = orders.get(i);

            List<Integer> operands = new ArrayList<>(fSet);
            operands.retainAll(pattern.getAdj(id));

            // automorphism conds
            List<Integer> condition = symmetryBreakingConditions.get(id);//pattern.getCondition(id);
            List<Integer> automorphismConds = new ArrayList<>();
            if(condition != null) {
                for (int c : condition) {
                    if (fSet.contains(Math.abs(c))) {
                        automorphismConds.add(c);
                    }
                }
            }

            // injective conds
            List<Integer> injectiveConds = new ArrayList<>(fSet);
            injectiveConds.removeAll(pattern.getAdj(id));
            injectiveConds.removeAll(automorphismConds);

            if (operands.size() == 0) {
                // fi is disconnected
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        ConstantCharacter.INTCANDIDATE + id,
                        ConstantCharacter.DATAVERTEXSET,
                        automorphismConds,
                        injectiveConds));
            }

            else if (operands.size() == 1) {
                // fi=Ax
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        ConstantCharacter.INTCANDIDATE + id,
                        ConstantCharacter.DBQTARGET + operands.get(0),
                        automorphismConds,
                        injectiveConds
                        /*-1*/));

            } else {
                // fi=Ax,Ay,...
                List<String> ops = new ArrayList<>();
                for (Integer op : operands) {
                    ops.add(ConstantCharacter.DBQTARGET + op);
                }
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        ConstantCharacter.INTTARGET + id, ops));

                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        ConstantCharacter.INTCANDIDATE + id,
                        ConstantCharacter.INTTARGET + id,
                        automorphismConds,
                        injectiveConds
                        /*-1*/));

            }
            execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.ENU,
                    ConstantCharacter.ENUTARGET + id, ConstantCharacter.INTCANDIDATE + id));
            execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                    ConstantCharacter.DBQTARGET + id, ConstantCharacter.ENUTARGET + id));
            fSet.add(id);
        }

        List<String> embedding = new ArrayList<String>();
        for (Integer fid : fSet) {
            embedding.add(ConstantCharacter.ENUTARGET + fid);
        }
        execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.RES,
                "f", embedding));

        computeExecutionPlanDependencies(execPlan);
        removeUnusedDBQInstructions(execPlan);

        return execPlan;
    }

    private void selectOptimalExecPlan() {
        String optimal = "";
        //long leastCost = Long.MAX_VALUE;
        BigInteger leastCost = BigInteger.valueOf(-1);
        int leastCostPlanDepth = Integer.MAX_VALUE;

        for(int i = 0; i < optimizedComputationalExecPlan.size(); i++) {
            List<ExecutionInstruction> execPlan = optimizedComputationalExecPlan.get(i);

            //long cost = getTotalComputationalCost(execPlan, optimizedPartialMatches.get(i));
            BigInteger cost = getTotalComputationalCost(execPlan);

            int depth = getPlanDepth(execPlan);

            if (ConstantCharacter.DEBUG) {
                System.out.print("matching order: ");
                for(int o: optimizedDBMatchingOrder.get(i)) {
                    System.out.print(o + " ");
                }
                System.out.println();
                System.out.println("compuataional cost: " + cost);
            }
            if(cost.compareTo(leastCost) < 0 || leastCost.compareTo(BigInteger.ZERO) < 0) {
                leastCost = cost;
                leastCostPlanDepth = depth;
                optimal = "" + i;
            } else if(cost.compareTo(leastCost) == 0) {
                //optimal = optimal + " " + i;
                if(depth < leastCostPlanDepth) {
                    optimal = "" + i;
                    leastCostPlanDepth = depth;
                } else if(depth == leastCostPlanDepth) {
                    optimal = optimal + " " + i;
                }
            }
        }

        System.out.println("the number of optimal execution plan: " + optimal.split(" ").length);

        if (ConstantCharacter.DEBUG) {
            System.out.println("*********************************************************************************");
            for (String i : optimal.split(" ")) {
                System.out.println("----------optimal execution plan---------------");
                List<ExecutionInstruction> executionPlan = optimizedComputationalExecPlan.get(Integer.parseInt(i));

                List<ExecutionInstruction> execCopy = new ArrayList<>();
                for(ExecutionInstruction instruction: executionPlan) {
                    execCopy.add(new ExecutionInstruction(instruction));
                }

                refineFilterCondition(executionPlan);

                for (ExecutionInstruction instruction : executionPlan) {
                    System.out.println(instruction);
                }
                if (ConstantCharacter.COMPRESS) {
                    System.out.println("---------optimal execution plan - compression-------------");
                    generateCompressedPlan(execCopy);
                    refineFilterCondition(execCopy);
                    for (ExecutionInstruction instruction : execCopy) {
                        System.out.println(instruction);
                    }
                }
                System.out.println("----------------------------------------------------------");
            }
        } else {
            System.out.println("Final Optimal Execution Plan");
            System.out.println("---------------------------------------------------");
            List<ExecutionInstruction> executionPlan = optimizedComputationalExecPlan.get(Integer.parseInt(optimal.split(" ")[0]));

            List<ExecutionInstruction> execCopy = new ArrayList<>();
            for(ExecutionInstruction instruction: executionPlan) {
                execCopy.add(new ExecutionInstruction(instruction));
            }

            refineFilterCondition(executionPlan);
            for (ExecutionInstruction instruction : executionPlan) {
                System.out.println(instruction);
            }

            if (ConstantCharacter.COMPRESS) {
                System.out.println("Final Optimal Execution Plan - Compression");
                System.out.println("---------------------------------------------------");
                generateCompressedPlan(execCopy);
                refineFilterCondition(execCopy);
                for (ExecutionInstruction instruction : execCopy) {
                    System.out.println(instruction);
                }
            }
        }
    }

    public BigInteger getTotalComputationalCost(List<ExecutionInstruction> execPlan) {
        Graph partialPattern = new Graph();
        BigInteger totalCost = BigInteger.ZERO;
        BigInteger partialMatchesNum = BigInteger.ZERO;
        int startVid = -1;
        for(ExecutionInstruction instruction: execPlan) {
            if(instruction.getType() == ExecutionInstruction.InstructionType.INI) {
                String target = instruction.getTargetVariable();
                int v = Integer.parseInt(target.substring(1, target.length()));
                partialPattern.addVertex(v);
                partialMatchesNum = BigInteger.valueOf(dataGraphVertexNum);
                startVid = v;
            }
            else if(instruction.getType() == ExecutionInstruction.InstructionType.ENU) {
                String target = instruction.getTargetVariable();
                int v = Integer.parseInt(target.substring(1, target.length()));
                updatePartialPattern(v, partialPattern.getAllVertices(), partialPattern);
                PartialPatternMatchesEstimator estimator = new PartialPatternMatchesEstimator(partialPattern);
                partialMatchesNum = estimator.computeCost(degArray, sumOfPower, startVid, dataGraphVertexNum);
            }
            else if(instruction.getType() == ExecutionInstruction.InstructionType.INT || instruction.getType() == ExecutionInstruction.InstructionType.TRC) {
                totalCost = totalCost.add(partialMatchesNum);
            }
        }
        return totalCost;
    }

    private int getPlanDepth(List<ExecutionInstruction> execPlan) {
        // we construct partial pattern that contains all dbq vertices and get the depth of partial pattern

        // construct partial pattern
        Graph partialPattern = new Graph();
        int startVid = -1;
        for(ExecutionInstruction instruction: execPlan) {
            if(instruction.getType() == ExecutionInstruction.InstructionType.INI) {
                String target = instruction.getTargetVariable();
                int v = Integer.parseInt(target.substring(1, target.length()));
                partialPattern.addVertex(v);
                startVid = v;
            }
            else if(instruction.getType() == ExecutionInstruction.InstructionType.DBQ) {
                String target = instruction.getTargetVariable();
                int v = Integer.parseInt(target.substring(1, target.length()));
                if(v != startVid) {
                    updatePartialPattern(v, partialPattern.getAllVertices(), partialPattern);
                }
            }
        }

        // get the depth of partial pattern
        partialPattern.initVertexMark();
        Queue<Integer> q = new LinkedList<Integer>();
        int depth = partialPattern.bfs(startVid, q);
        //System.out.println("partialPattern");
        //partialPattern.print();

        return depth;
    }

    private void computeExecutionPlanDependencies(List<ExecutionInstruction> execPlan) {
        Map<String, HashSet<String>> dependRecord = new HashMap<String, HashSet<String>>();
        for (ExecutionInstruction instruction : execPlan) {
            HashSet<String> dependSet = new HashSet<String>();
            if (instruction.isSingleOperator()) {
                String op = instruction.getSingleOperator();
                if(!op.equals(ConstantCharacter.DATAVERTEXSET)) {
                    dependSet.add(op);
                }
                if (dependRecord.containsKey(op)) {
                    dependSet.addAll(dependRecord.get(op));
                }
                if (!instruction.getAutomorphismBreakingConds().isEmpty()) {
                    for (Integer cond : instruction.getAutomorphismBreakingConds()) {
                        op = ConstantCharacter.ENUTARGET + Math.abs(cond);
                        dependSet.add(op);
                        if (dependRecord.containsKey(op)) {
                            dependSet.addAll(dependRecord.get(op));
                        }
                    }
                }
                if (!instruction.getInjectiveConds().isEmpty()) {
                    for (Integer cond : instruction.getInjectiveConds()) {
                        op = ConstantCharacter.ENUTARGET + cond;
                        dependSet.add(op);
                        if (dependRecord.containsKey(op)) {
                            dependSet.addAll(dependRecord.get(op));
                        }
                    }
                }
            } else {
                dependSet.addAll(instruction.getMultiOperators());
                for (String ops : instruction.getMultiOperators()) {
                    if (dependRecord.containsKey(ops)) {
                        dependSet.addAll(dependRecord.get(ops));
                    }
                }
            }
            dependRecord.put(instruction.getTargetVariable(), dependSet);
            ArrayList<String> dependOn = new ArrayList<String>();
            dependOn.addAll(dependSet);
            instruction.setDependOn(dependOn);
        }
    }

    private void removeUnusedDBQInstructions(List<ExecutionInstruction> execPlan) {
        HashSet<String> dependSet = new HashSet<String>();
        for (ExecutionInstruction instruction : execPlan) {
            if (instruction.isSingleOperator()) {
                dependSet.add(instruction.getSingleOperator());
            } else {
                dependSet.addAll(instruction.getMultiOperators());
            }
        }

        Iterator<ExecutionInstruction> iter = execPlan.iterator();
        while (iter.hasNext()) {
            ExecutionInstruction instruction = iter.next();
            if (instruction.getType() == ExecutionInstruction.InstructionType.DBQ &&
                    !dependSet.contains(instruction.getTargetVariable())) {
                iter.remove();
            }
        }
    }

    private void eliminateCommonSubExpression(List<ExecutionInstruction> execPlan) {
        Ti = pattern.getVertexNum();
        while (true) {
            List<HashSet<String>> dataList = new ArrayList<HashSet<String>>();
            List<Integer> instructionIndex = new ArrayList<Integer>();

            Map<String, Integer> INTOperatorsPos = new HashMap<>();

            // 1. get INT instructions, store operators
            ExecutionInstruction instruction;
            for (int i = 0; i < execPlan.size(); i++) {
                instruction = execPlan.get(i);
                //System.out.println(instruction);
                if (instruction.getType() == ExecutionInstruction.InstructionType.DBQ) {
                    INTOperatorsPos.put(instruction.getTargetVariable(), i); // Ax
                }
                if (instruction.getType() == ExecutionInstruction.InstructionType.INT && !instruction.isSingleOperator()) {
                    INTOperatorsPos.put(instruction.getTargetVariable(), i); // Tx

                    HashSet<String> operators = new HashSet<String>(instruction.getMultiOperators());
                    /*
                    for (String op : instruction.getMultiOperators()) {
                        operators.add(op);
                    }*/
                    dataList.add(operators);
                    instructionIndex.add(i);
                }
            }

            HashSet<String> maxFrequentSet = new HashSet<String>();
            int maxFrequentSetSize = 0;
            int maxFrequentSupport = 0;

            // 2. find frequentSet that has max size and support>=2
            Apriori apriori = new Apriori(dataList, 2);
            //Map<HashSet<String>, Integer> frequentSet = apriori.generateAllFrequentSet();
            Map<HashSet<String>, Integer> frequentSet = apriori.generateMaxSizeFrequentSet();

            // 3. find maximal frequentSet that has max support
            for (Map.Entry<HashSet<String>, Integer> entry : frequentSet.entrySet()) {
                HashSet<String> key = entry.getKey();
                Integer value = entry.getValue();
                if (value > maxFrequentSupport) {
                    maxFrequentSet = key;
                    maxFrequentSetSize = key.size();
                    maxFrequentSupport = value;
                }
                else if (value == maxFrequentSupport) {
                    // compare the position of maxFrequentSet and key, pick the one appearing first
                    List<Integer> l1 = new ArrayList<>();
                    List<Integer> l2 = new ArrayList<>();
                    for (String x : maxFrequentSet) {
                        l1.add(INTOperatorsPos.get(x));
                    }
                    for (String x : key) {
                        l2.add(INTOperatorsPos.get(x));
                    }
                    Collections.sort(l1);
                    Collections.sort(l2);
                    for (int i = 0; i < l1.size(); i++) {
                        if(l1.get(i) > l2.get(i)) {
                            maxFrequentSet = key;
                            maxFrequentSetSize = key.size();
                            maxFrequentSupport = value;
                        }
                    }
                }
            }

            // 4. eliminate common subexpression
            if (maxFrequentSetSize >= 2) {
                Ti++;
                boolean flag = true; // true if frequent item set appear firstly.

                for (int i = 0; i < dataList.size(); i++) {
                    HashSet<String> key = dataList.get(i);
                    if (key.containsAll(maxFrequentSet)) {
                        key.add(ConstantCharacter.INTTARGET + Ti);
                        key.removeAll(maxFrequentSet);
                        if (flag) {
                            List<String> operators = new ArrayList<>(maxFrequentSet);
                            operators.sort((a,b) -> INTOperatorsPos.get(a).compareTo(INTOperatorsPos.get(b)));

                            ExecutionInstruction newInstruction = new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                    ConstantCharacter.INTTARGET + Ti, operators);
                            execPlan.add(instructionIndex.get(i), newInstruction);
                            flag = false;
                        }
                        if (key.size() > 1) {
                            execPlan.get(instructionIndex.get(i) + 1).setMultiOperators(new ArrayList<>(key));
                        } else {
                            Iterator keyIter = key.iterator();
                            while (keyIter.hasNext()) {
                                execPlan.get(instructionIndex.get(i) + 1).setSingleOperator((String) keyIter.next());
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        // 5. remove single operator intersect instruction without any filter condition. X=Intersect(Y)
        Iterator listIter = execPlan.iterator();
        Map<String, String> record = new HashMap<String, String>();
        while (listIter.hasNext()) {
            ExecutionInstruction instruction = (ExecutionInstruction) listIter.next();
            if (instruction.getType() != ExecutionInstruction.InstructionType.INI &&
                    instruction.getType() != ExecutionInstruction.InstructionType.RES) {
                if (instruction.getType() == ExecutionInstruction.InstructionType.INT &&
                        instruction.isSingleOperator() &&
                        instruction.getTargetVariable().startsWith(ConstantCharacter.INTTARGET)) {
                    record.put(instruction.getTargetVariable(), instruction.getSingleOperator());
                    listIter.remove();
                } else {
                    if (instruction.isSingleOperator()) {
                        if (record.containsKey(instruction.getSingleOperator())) {
                            instruction.setSingleOperator(record.get(instruction.getSingleOperator()));
                        }
                    } else {
                        ArrayList<String> tempOps = new ArrayList<String>();
                        tempOps.addAll(instruction.getMultiOperators());
                        for (String op : tempOps) {
                            if (record.containsKey(op)) {
                                tempOps.add(record.get(op));
                                tempOps.remove(op);
                            }
                        }
                        instruction.setMultiOperators(tempOps);
                    }

                }
            }
        }

        if (ConstantCharacter.DEBUG) {
            System.out.println("**************eliminate***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private void flatten(List<ExecutionInstruction> execPlan) {
        List<Integer> instructionIndex = new ArrayList<Integer>();
        List<String> definedVariables = new ArrayList<String>();
        for (int i = 0; i < execPlan.size(); i++) {
            ExecutionInstruction instruction = execPlan.get(i);
            definedVariables.add(instruction.getTargetVariable());
            if (instruction.getType() == ExecutionInstruction.InstructionType.INT && instruction.getMultiOperators().size() > 2) {
                instructionIndex.add(i);
            }
        }

        int offset = 0;
        for (Integer index : instructionIndex) {
            // mark position before instruction because insert and remove operation change position
            int position = index + offset - 1;
            // for sort the operators according to the positions where they are defined
            List<String> operators = new ArrayList<String>();
            operators.addAll(definedVariables);
            operators.retainAll(execPlan.get(position + 1).getMultiOperators());

            while (operators.size() > 2) {
                //System.out.println("position: " + position);
                ArrayList<String> newOperators = new ArrayList<String>();
                String op1 = operators.get(0);
                String op2 = operators.get(1);
                newOperators.add(op1);
                newOperators.add(op2);
                operators.remove(op1);
                operators.remove(op2);
                Ti++;
                operators.add(0, ConstantCharacter.INTTARGET + Ti);
                ExecutionInstruction newinstruction = new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        ConstantCharacter.INTTARGET + Ti, newOperators);
                execPlan.add(++position, newinstruction);
                offset++;

            }
            //System.out.println("operators.size(): " + operators.size());
            ArrayList<String> newOperators = new ArrayList<String>();
            newOperators.add(operators.get(0));
            newOperators.add(operators.get(1));
            execPlan.get(++position).setMultiOperators(newOperators);
        }
    }

    private void reorder(List<ExecutionInstruction> execPlan) {
        computeExecutionPlanDependencies(execPlan);
        /*
        System.out.println("*************computeExecutionPlanDependencies***************");
        for(ExecutionInstruction instruction: execPlan) {
            System.out.print(instruction);
            System.out.print("///DEPEND: ");
            for(String depend: instruction.getDependOn()) {
                System.out.print(depend + ",");
            }
            System.out.println();
        }
        */
        HashSet<String> certainSet = new HashSet<String>();
        certainSet.add(execPlan.get(0).getTargetVariable());
        for (int i = 1; i < execPlan.size() - 1; i++) {
            ArrayList<Integer> candidates = new ArrayList<Integer>();

            // find candidate instructions
            for (int j = i; j < execPlan.size() - 1; j++) {
                ExecutionInstruction instruction = execPlan.get(j);
                if (certainSet.containsAll(instruction.getDependOn())) {
                    candidates.add(j);
                }
            }

            // pick the smallest instruction to move
            int insPosition = -1;
            int dependencySetSize = Integer.MAX_VALUE;
            if (!candidates.isEmpty()) {
                insPosition = candidates.get(0);
                if (candidates.size() > 1) {
                    for (int j = 1; j < candidates.size(); j++) {
                        int index = candidates.get(j);
                        if (execPlan.get(index).getType().compareTo(execPlan.get(insPosition).getType()) < 0) {
                            insPosition = index;
                        }
                    }
                }
            }

            //move the ins forward to the i-th position
            if (insPosition > 0) {
                certainSet.add(execPlan.get(insPosition).getTargetVariable());
                execPlan.add(i, execPlan.get(insPosition));
                execPlan.remove(insPosition + 1);
            }
            certainSet.add(execPlan.get(i).getTargetVariable());

        }

        if (ConstantCharacter.DEBUG) {
            System.out.println("**************reorder***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private void substituteVariable(List<ExecutionInstruction> execPlan) {
        Map<String, HashSet<String>> tRecord = new HashMap<String, HashSet<String>>();

        for (int i = 0; i < execPlan.size(); i++) {
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getType() == ExecutionInstruction.InstructionType.INT && !instruction.isSingleOperator()) {
                // X=Intersect(A,B)
                HashSet<String> operators = new HashSet<String>();
                operators.addAll(instruction.getMultiOperators());

                boolean flag = false;

                Iterator opIter = operators.iterator();
                ArrayList<String> tAdd = new ArrayList<String>();
                while (opIter.hasNext()) {
                    String op = (String) opIter.next();
                    if (tRecord.containsKey(op)) {
                        tAdd.add(op);
                        opIter.remove();
                        flag = true;
                    }
                }
                for (String t : tAdd) {
                    operators.addAll(tRecord.get(t));
                }

                if (flag) {
                    for (Map.Entry<String, HashSet<String>> entry : tRecord.entrySet()) {
                        String key = entry.getKey();
                        HashSet<String> value = entry.getValue();
                        if (value.equals(operators)) {
                            ArrayList<String> newOperators = new ArrayList<String>();
                            for (String op : key.split(",")) {
                                newOperators.add(op);
                            }
                            instruction.setMultiOperators(newOperators);
                        }
                    }
                }

                HashMap<String, HashSet<String>> joinOpertion = new HashMap<String, HashSet<String>>();

                for (Map.Entry<String, HashSet<String>> entry : tRecord.entrySet()) {
                    String key = entry.getKey();
                    HashSet<String> value = entry.getValue();

                    String newKey = key + "," + instruction.getTargetVariable();
                    HashSet<String> newValue = new HashSet<String>();
                    newValue.addAll(value);
                    newValue.addAll(operators);

                    joinOpertion.put(newKey, newValue);
                }
                tRecord.put(instruction.getTargetVariable(), operators);
                tRecord.putAll(joinOpertion);

            }
        }

        if (ConstantCharacter.DEBUG) {
            System.out.println("**************substituteVariable***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private void cacheTriangle(List<ExecutionInstruction> execPlan) {
        String start = "";
        int startId = -1;
        String index = "";
        int loopLevel = 0; // mark the loop level
        int tCacheLoopLevel = 0;
        for (int i = 0; i < execPlan.size(); i++) {
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getType() == ExecutionInstruction.InstructionType.INI) {
                start = instruction.getTargetVariable();
                startId = Integer.parseInt(start.substring(1, start.length()));
            } else if (instruction.getType() == ExecutionInstruction.InstructionType.ENU) {
                loopLevel++;
            } else if (instruction.getType() == ExecutionInstruction.InstructionType.INT && !instruction.isSingleOperator()) {
                String op1 = instruction.getMultiOperators().get(0);
                String op2 = instruction.getMultiOperators().get(1);
                if (op1.startsWith(ConstantCharacter.DBQTARGET) && op2.startsWith(ConstantCharacter.DBQTARGET)) {
                    // X=Intersect(Ni,Nj)
                    int id1 = Integer.parseInt(op1.substring(1, op1.length()));
                    int id2 = Integer.parseInt(op2.substring(1, op2.length()));
                    if ((id1 == startId && pattern.getAdj(id1).contains(id2)) ||
                            id2 == startId && pattern.getAdj(id2).contains(id1)) {
                        index += i + ",";
                        tCacheLoopLevel = loopLevel;
                    }
                }
            }
        }
        if (index.split(",").length > 1 || tCacheLoopLevel > 1) {
            for (String i : index.split(",")) {
                ExecutionInstruction instruction = execPlan.get(Integer.parseInt(i));
                String op1 = instruction.getMultiOperators().get(0);
                String op2 = instruction.getMultiOperators().get(1);
                int id1 = Integer.parseInt(op1.substring(1, op1.length()));
                int id2 = Integer.parseInt(op2.substring(1, op2.length()));
                ArrayList<String> newOperators = new ArrayList<String>();
                newOperators.add(ConstantCharacter.ENUTARGET + id1);
                newOperators.add(ConstantCharacter.ENUTARGET + id2);
                newOperators.addAll(instruction.getMultiOperators());
                instruction.setMultiOperators(newOperators);
                instruction.setType(ExecutionInstruction.InstructionType.TRC);
            }
        }

        if (ConstantCharacter.DEBUG) {
            System.out.println("**************cacheTriangle***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private void addFilteringCondition(List<ExecutionInstruction> execPlan) {
        //promoteAutomorphismBreakingCondidtion(execPlan);
        if(ConstantCharacter.DEGREEFILTER) {
            addDegreeCondition(execPlan);
        }
        computeDBQTargetVariableSize(execPlan);
        computeINTTargetVariableSize(execPlan);
    }

    private void promoteAutomorphismBreakingCondidtion(List<ExecutionInstruction> execPlan) {
        // 1. construct a dependency graph between Ci, Ti and Ni variables
        DiGraph dependencyGraph = new DiGraph();
        for (ExecutionInstruction instruction : execPlan) {
            if (instruction.getType() == ExecutionInstruction.InstructionType.INT || instruction.getType() == ExecutionInstruction.InstructionType.TRC) {
                if (instruction.isSingleOperator()) {
                    String op = instruction.getSingleOperator();
                    String target = instruction.getTargetVariable();
                    dependencyGraph.addEdge(op, target);
                    dependencyGraph.addCondition(target, instruction.getAutomorphismBreakingConds());
                } else {
                    String target = instruction.getTargetVariable();
                    for (String op : instruction.getMultiOperators()) {
                        if (!op.startsWith(ConstantCharacter.ENUTARGET)) {
                            dependencyGraph.addEdge(op, target);
                            dependencyGraph.addCondition(target, instruction.getAutomorphismBreakingConds());
                        }
                    }
                }
            }
        }
        //dependencyGraph.print();

        // 2. topological sort
        List<String> topSeq = dependencyGraph.topsort();

        // 3. scan the variables in the topological order of the dependency graph, promote the Automorphism breaking condidtion
        for (String v : topSeq) {
            boolean flag = true;
            HashSet<Integer> commoncondition = new HashSet<Integer>();
            for (String child : dependencyGraph.getAdj(v)) {
                List<Integer> condition = dependencyGraph.getCondition(child);
                if (condition == null) {
                    break;
                } else {
                    if (flag) {
                        commoncondition.addAll(condition);
                        flag = false;
                    } else {
                        commoncondition.retainAll(condition);
                    }
                }
                if (commoncondition.isEmpty()) {
                    break;
                }

            }
            if (!commoncondition.isEmpty()) {
                dependencyGraph.addCondition(v, new ArrayList<Integer>(commoncondition));
            }
        }
        // remove the Automorphism breaking condidtion of children same to parents
        for (String v : topSeq) {
            List<Integer> commoncondition = dependencyGraph.getCondition(v);
            if (commoncondition != null) {
                for (String child : dependencyGraph.getAdj(v)) {
                    dependencyGraph.removeCondition(child, commoncondition);
                }
            }
        }
        // update the Automorphism breaking condidtion of execution plan
        for (ExecutionInstruction instruction : execPlan) {
            List<Integer> condition = dependencyGraph.getCondition(instruction.getTargetVariable());
            if (condition != null) {
                instruction.setAutomorphismBreakingConds(condition);
            }
        }
        /*
        if(ConstantCharacter.DEBUG) {
            System.out.println("**************promoteAutomorphismBreakingCondidtion***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }*/
    }

    private void addDegreeCondition(List<ExecutionInstruction> execPlan) {
        HashSet<String> confirmedC = new HashSet<>();
        for (int i = 0; i < execPlan.size() - 1; i++) {
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getType() == ExecutionInstruction.InstructionType.INI ||
                    instruction.getType() == ExecutionInstruction.InstructionType.ENU) {
                confirmedC.add(instruction.getTargetVariable());
            } else if (instruction.getType() == ExecutionInstruction.InstructionType.INT &&
                    instruction.isSingleOperator()) {
                String target = instruction.getTargetVariable();
                int vid = Integer.parseInt(target.substring(1, target.length()));
                //System.out.println(target);
                //System.out.println(pattern.getDegree(vid));
                //System.out.println(getINTInstructionDependencySet(execPlan, target, i).size());
                if (pattern.getDegree(vid) >= 3 &&
                        getINTInstructionDependencySet(execPlan, target, i).size() < pattern.getDegree(vid)) {
                    instruction.setDegreeCondition(vid);
                    //System.out.println("set " + target);
                }
            }
        }
    }

    private void computeDBQTargetVariableSize(List<ExecutionInstruction> execPlan) {
        for (ExecutionInstruction instruction : execPlan) {
            if (instruction.getType() == ExecutionInstruction.InstructionType.DBQ) {
                int vid = Integer.parseInt(instruction.getTargetVariable().substring(1));
                int degree = pattern.getDegree(vid);
                instruction.setSizeCondition(degree);
            }
        }
    }

    private void computeINTTargetVariableSize(List<ExecutionInstruction> execPlan) {
        HashMap<String, Integer> tCounter = new HashMap<String, Integer>();
        for (int i = 0; i < execPlan.size() - 1; i++) {
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getType() == ExecutionInstruction.InstructionType.ENU) {
                for (String dependOn : instruction.getDependOn()) {
                    if (dependOn.startsWith(ConstantCharacter.INTTARGET)) {
                        tCounter.put(dependOn, tCounter.getOrDefault(dependOn, 0) + 1);
                    }
                }
            }
        }

        for (int i = 0; i < execPlan.size(); i++) {
            boolean filterFlag = false; // mark INT instruction whether has filter condition
            HashSet<String> intersectRawNSet = new HashSet<String>(); // T1=Intersect(N1, N2); T2=Intersect(T1, N3) => T2: N1, N2, N3
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getType() == ExecutionInstruction.InstructionType.INT ||
                    instruction.getType() == ExecutionInstruction.InstructionType.TRC &&
                            !instruction.isSingleOperator()) {
                for (String op : instruction.getMultiOperators()) {
                    if (op.startsWith(ConstantCharacter.INTTARGET) || op.startsWith(ConstantCharacter.DBQTARGET)) {
                        if (op.startsWith(ConstantCharacter.DBQTARGET)) {
                            intersectRawNSet.add(op);
                        }
                        filterFlag = hasINTFilter(op, i - 1, execPlan, intersectRawNSet);
                    }
                }
                if (filterFlag) { // if has filter condition
                    String targetV = instruction.getTargetVariable();
                    if (tCounter.containsKey(targetV) && tCounter.get(targetV) > 1) {
                        instruction.setSizeCondition(tCounter.get(targetV));
                    }
                } else {
                    // get common vertex num in pattern graph for intersect operation
                    // System.out.println("intersectRawNSet: " + intersectRawNSet);
                    ArrayList<Integer> commonVertex = new ArrayList<Integer>();
                    boolean firstFlag = true; // mark the first
                    for (String op : intersectRawNSet) {
                        if (firstFlag) {
                            commonVertex.addAll(pattern.getAdj(Integer.parseInt(op.substring(1, op.length()))));
                            firstFlag = false;
                        } else {
                            commonVertex.retainAll(pattern.getAdj(Integer.parseInt(op.substring(1, op.length()))));
                        }
                    }
                    instruction.setSizeCondition(commonVertex.size());
                }
            }
        }

        if (ConstantCharacter.DEBUG) {
            System.out.println("**************addFilteringCondition***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private boolean hasINTFilter(String target, int position, List<ExecutionInstruction> execPlan, Set<String> intersectRawNSet) {
        boolean flag = false;
        for (int i = position; i >= 0; i--) {
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getTargetVariable().equals(target)) {
                if (instruction.hasFilterCondition()) {
                    flag = true;
                } else {
                    if (instruction.getTargetVariable().startsWith(ConstantCharacter.DBQTARGET)) {
                        flag = false;
                    } else {
                        for (String op : instruction.getMultiOperators()) {
                            if (op.startsWith(ConstantCharacter.DBQTARGET)) {
                                intersectRawNSet.add(op);
                            }
                            flag = hasINTFilter(op, i - 1, execPlan, intersectRawNSet);
                        }
                    }
                }
                break;
            }
        }
        return flag;
    }

    private void refineFilterCondition(List<ExecutionInstruction> execPlan) {
        // (1) >n, ≠n => >n；<n, ≠n => <n
        // (2) >n1, >n2 and n1 > n2 or n1 < n2 => >max(n1, n2)
        // (3) <n1, <n2 and n1 > n2 or n1 < n2 => <min(n1, n2)
        Map<Integer, List<Integer>> condsRecodes = new HashMap<>(); // store f and the automorphism breaking conditions
        for(ExecutionInstruction instruction: execPlan) {
            if(instruction.getType() == ExecutionInstruction.InstructionType.INT) {
                List<Integer> autoBreakingConds = instruction.getAutomorphismBreakingConds();
                if(!autoBreakingConds.isEmpty()) {
                    // put f and the automorphism breaking conditions into condsRecodes
                    List<Integer> conds = new ArrayList<>();
                    conds.addAll(autoBreakingConds);
                    condsRecodes.put(Integer.parseInt(instruction.getTargetVariable().substring(1)), conds);
                    // (1) remove redundant injective conditions(>n, ≠n => >n；<n, ≠n => <n)
                    List<Integer> injectiveCondes = instruction.getInjectiveConds();
                    for(int c: autoBreakingConds) {
                        if(injectiveCondes.contains(Math.abs(c))) {
                            injectiveCondes.remove(new Integer(Math.abs(c)));
                        }
                    }
                    instruction.setInjectiveConds(injectiveCondes);
                    // (2)
                    List<Integer> newAutoBreakingConds = new ArrayList<>();
                    newAutoBreakingConds.addAll(autoBreakingConds);
                    for(int i = 0; i < autoBreakingConds.size() - 1; i++) {
                        int c1 = autoBreakingConds.get(i);
                        for(int j = 0; j < autoBreakingConds.size(); j++) {
                            int c2 = autoBreakingConds.get(j);
                            if((c1 > 0 && c2 > 0) || (c1 < 0 && c2 < 0)) {
                                if(condsRecodes.getOrDefault(Math.abs(c1), new ArrayList<>()).contains(c2) ||
                                        condsRecodes.getOrDefault(Math.abs(c2), new ArrayList<>()).contains(-c1)) {
                                    newAutoBreakingConds.remove(new Integer(c2));
                                } else if(condsRecodes.getOrDefault(Math.abs(c2), new ArrayList<>()).contains(c1) ||
                                        condsRecodes.getOrDefault(Math.abs(c1), new ArrayList<>()).contains(-c2)) {
                                    newAutoBreakingConds.remove(new Integer(c1));
                                }
                            }
                            /*
                            // (2) >n1, >n2 and n1 > n2 or n1 < n2 => >max(n1, n2)
                            if(c1 > 0 && c2 > 0) {
                                if(condsRecodes.get(c1).contains(c2) || condsRecodes.get(c2).contains(-c1)) {
                                    // > c1
                                    newAutoBreakingConds.remove(c2);
                                } else if(condsRecodes.get(c2).contains(c1) || condsRecodes.get(c1).contains(-c2)) {
                                    // > c2
                                    newAutoBreakingConds.remove(c1);
                                }
                            } else if(c1 < 0 && c2 < 0) {
                                // (3) <n1, <n2 and n1 > n2 or n1 < n2 => <min(n1, n2)
                                if(condsRecodes.get(Math.abs(c1)).contains(c2) || condsRecodes.get(Math.abs(c2)).contains(-c1)) {
                                    // < c1
                                    newAutoBreakingConds.remove(c2);
                                } else if(condsRecodes.get(Math.abs(c2)).contains(c1) || condsRecodes.get(Math.abs(c1)).contains(-c2)) {
                                    // < c2
                                    newAutoBreakingConds.remove(c1);
                                }
                            }*/
                        }
                    }
                    instruction.setAutomorphismBreakingConds(newAutoBreakingConds);
                }
            }
        }
    }

    private HashSet<String> getINTInstructionDependencySet(List<ExecutionInstruction> execPlan, String targetV, int index) {
        HashSet<String> dependSet = new HashSet<String>();
        for (int i = index; i > 0; i--) {
            ExecutionInstruction instruction = execPlan.get(i);
            if (instruction.getTargetVariable().equals(targetV)) {
                if (instruction.isSingleOperator()) {
                    String op = instruction.getSingleOperator();
                    if (op.startsWith(ConstantCharacter.INTTARGET)) {
                        dependSet.addAll(getINTInstructionDependencySet(execPlan, op, i));
                    } else if (op.startsWith(ConstantCharacter.DBQTARGET)) {
                        dependSet.add(op);
                    }
                } else {
                    for (String op : instruction.getMultiOperators()) {
                        if (op.startsWith(ConstantCharacter.INTTARGET)) {
                            dependSet.addAll(getINTInstructionDependencySet(execPlan, op, i));
                        } else if (op.startsWith(ConstantCharacter.DBQTARGET)) {
                            dependSet.add(op);
                        }
                    }
                }
            }
        }
        return dependSet;
    }

    private void generateCompressedPlan(List<ExecutionInstruction> execPlan) {
        pattern.initEdgeMark();
        HashSet<Integer> nonCoreId = new HashSet<>();
        List<String> reportMatch = new ArrayList<>();

        Iterator<ExecutionInstruction> iter = execPlan.iterator();

        while (iter.hasNext()) {
            ExecutionInstruction instruction = iter.next();
            if (instruction.getType() == ExecutionInstruction.InstructionType.RES) {
                instruction.setMultiOperators(reportMatch);
            } else {
                int vid = Integer.parseInt(instruction.getTargetVariable().substring(1));
                if (instruction.getType() == ExecutionInstruction.InstructionType.INI) {
                    pattern.markCoveredEdges(vid);
                    reportMatch.add(ConstantCharacter.ENUTARGET + vid);
                }
                if (instruction.getType() == ExecutionInstruction.InstructionType.ENU) {
                    if (pattern.isGraphCovered()) {
                        nonCoreId.add(vid);
                        iter.remove();
                        reportMatch.add(ConstantCharacter.INTCANDIDATE + vid);
                    } else {
                        // mark covered edges by fi
                        pattern.markCoveredEdges(vid);
                        reportMatch.add(ConstantCharacter.ENUTARGET + vid);
                    }
                } else {
                    if (nonCoreId.contains(vid)) {
                        iter.remove();
                    } else {
                        List<Integer> autoConds = instruction.getAutomorphismBreakingConds();
                        autoConds.removeAll(nonCoreId);
                        instruction.setAutomorphismBreakingConds(autoConds);
                        List<Integer> injeConds = instruction.getInjectiveConds();
                        injeConds.removeAll(nonCoreId);
                        instruction.setInjectiveConds(injeConds);
                    }
                }
            }
        }

    }

    public void genDegreeSequence(String filePath) {
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                int degree = line.split(ConstantCharacter.DATAGRAPH).length - 1;
                degArray.add(degree);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        Collections.sort(degArray);
    }

    public void calSumOfPower(int upperExp) {
        for (int i = 1; i < upperExp; i++) {
            //double s = 0.0;
            BigDecimal s = BigDecimal.ZERO;
            for (int deg : degArray) {
                //s += Math.pow(deg, i);
                s = s.add(BigDecimal.valueOf(deg).pow(i));
            }
            sumOfPower.put(i, s);
        }
    }

    public void setDataGraphSize(long size) {
        dataGraphVertexNum = size;
    }

    public void buildPatternGraph() {
        try (BufferedReader br = new BufferedReader(new FileReader(patternGraphFile))) {
            String line = br.readLine();
            if(line != null) {
                int n = Integer.parseInt(line.split(" ")[0]);
                int m = Integer.parseInt(line.split(" ")[1]);
                int k = Integer.parseInt(line.split(" ")[2]);
                for(int i = 0; i < m; i++) {
                    line = br.readLine();
                    int x = Integer.parseInt(line.split(" ")[0]);
                    int y = Integer.parseInt(line.split(" ")[1]);
                    pattern.addEdge(x, y);
                }
                for(int i = 0; i < k; i++) {
                    line = br.readLine();
                    int a = Integer.parseInt(line.split(" ")[0]);
                    int b = Integer.parseInt(line.split(" ")[1]);
                    addCondition(symmetryBreakingConditions, a, b);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addCondition(Map<Integer, List<Integer>> conditions, int left, int right) {
        List<Integer> leftCond = conditions.getOrDefault(left, new ArrayList<>());
        List<Integer> rightCond = conditions.getOrDefault(right, new ArrayList<>());
        leftCond.add(-right);
        rightCond.add(left);
        conditions.put(left, leftCond);
        conditions.put(right, rightCond);
    }
}