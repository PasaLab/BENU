package cn.edu.nju.pasa.graph.plangen.incremental;

import cn.edu.nju.pasa.graph.plangen.Apriori;
import cn.edu.nju.pasa.graph.plangen.ExecutionInstruction;
import cn.edu.nju.pasa.graph.plangen.Graph;
import cn.edu.nju.pasa.graph.plangen.PartialPatternMatchesEstimator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class IncrementalPlanGen {
    private String patternGraphFile = "";
    private DiGraph pattern = new DiGraph();
    private ArrayList<Integer> degArray = new ArrayList<>();
    private HashMap<Integer, BigDecimal> sumOfPower = new HashMap<>();
    private long dataGraphVertexNum = 0;
    private long dataGraphEdgeNum = 0;
    private int Ti = 0;
    private BigInteger optimalDBCost = BigInteger.valueOf(-1);

    private List<List<Integer>> optimizedDBMatchingOrder = new ArrayList<>();
    // store all the optimal matching order optimized db costs. each element is like <u1, u2, u3, ...>

    private List<List<ExecutionInstruction>> optimizedComputationalExecPlan = new ArrayList<>();
    // store all the execution plan optimized computational costs

    private int costEstimateCounter = 0;

    private int totalOptimizedDBMatchingOrderSize = 0;
    private int totalCostEstimateCounter = 0;

    private List<int[]> patternEdgeOrder = new ArrayList<>();

    public IncrementalPlanGen(String patternGraphFile) {
        this.patternGraphFile = patternGraphFile;
    }

    public int getCostEstimateCounter() {return totalCostEstimateCounter;}

    public int getOptimizedDBMatchingOrderSize() {return totalOptimizedDBMatchingOrderSize;}

    public int getPatternGraphsEdgesNum() {return patternEdgeOrder.size();}

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
                    patternEdgeOrder.add(new int[]{x, y});
                }
                for(int i = 0; i < k; i++) {
                    line = br.readLine();
                    int a = Integer.parseInt(line.split(" ")[0]);
                    int b = Integer.parseInt(line.split(" ")[1]);
                    pattern.addCondition(a, b);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generateOptimalPlan() throws Exception{
        generateDynamicOptimalPlan();
    }

    private void generateDynamicOptimalPlan() throws Exception{
        long startTime = System.currentTimeMillis();
        List<Integer> vertexList = new ArrayList<>(pattern.getAllVertices());
        Collections.sort(vertexList);

        for(int i = 0; i < patternEdgeOrder.size(); i++) {
            optimalDBCost = BigInteger.valueOf(-1);
            optimizedDBMatchingOrder.clear();
            optimizedComputationalExecPlan.clear();
            costEstimateCounter = 0;

            List<Integer> matchedVertex = new ArrayList<>();
            Graph partialPattern = new Graph();
            List<BigInteger> estimatedCost = new ArrayList<>();

            int src = patternEdgeOrder.get(i)[0];
            int dst = patternEdgeOrder.get(i)[1];

            System.out.println("Delta edge: (" + src + ", " + dst + ")");

            // 0. calculate equivalent classes of pattern vertexes
            List<List<Integer>> equivalentVertex = new ArrayList<>();
            for(int m = 0; m < vertexList.size()-1; m++) {
                int v1 = vertexList.get(m);
                for (int n = m+1; n < vertexList.size(); n++) {
                    boolean flag = true;
                    int v2 = vertexList.get(n);
                    List<Integer> inAdj1 = new ArrayList<>(pattern.getInAdj(v1));
                    List<Integer> inAdj2 = new ArrayList<>(pattern.getInAdj(v2));
                    inAdj1.remove(new Integer(v2));
                    inAdj2.remove(new Integer(v1));
                    if(inAdj1.size() != inAdj2.size() || !inAdj1.containsAll(inAdj2))
                        continue;
                    for(int x : inAdj1) { // check the type of edge
                        if(getPatternEdgeType(src, dst, x, v1) != getPatternEdgeType(src, dst, x, v2)) {
                            flag = false;
                            break;
                        }
                    }
                    if(flag) {
                        List<Integer> outAdj1 = new ArrayList<>(pattern.getOutAdj(v1));
                        List<Integer> outAdj2 = new ArrayList<>(pattern.getOutAdj(v2));
                        outAdj1.remove(new Integer(v2));
                        outAdj2.remove(new Integer(v1));
                        if(outAdj1.size() != outAdj2.size() || !outAdj1.containsAll(outAdj2))
                            continue;
                        for(int x : outAdj1) { // check the type of edge
                            if(getPatternEdgeType(src, dst, v1, x) != getPatternEdgeType(src, dst, v2, x)) {
                                flag = false;
                                break;
                            }
                        }
                    }
                    if(flag) {
                        List<Integer> s = new ArrayList<>();
                        s.add(v1);
                        s.add(v2);
                        equivalentVertex.add(s);
                        break;
                    }
                }
            }

            if(InstructionNotation.DEBUG) {
                System.out.println("equivalent vertex set");
                for (List<Integer> s : equivalentVertex) {
                    for (int v : s) {
                        System.out.print(v + " ");
                    }
                    System.out.println();
                }
            }

            matchedVertex.add(src);
            matchedVertex.add(dst);
            partialPattern.addEdge(src, dst);
            estimatedCost.add(BigInteger.valueOf(dataGraphVertexNum));
            estimatedCost.add(BigInteger.valueOf(dataGraphEdgeNum));

            for (int j = 0; j < vertexList.size(); j++) {
                if (!matchedVertex.contains(vertexList.get(j))) {
                    enumerateDynamicOrder(vertexList.get(j), j, vertexList, matchedVertex, partialPattern, estimatedCost,
                            equivalentVertex);
                }
            }

            if(InstructionNotation.DEBUG ) {
                System.out.println("Optimized DBCost Matching Order: ");
                for(List<Integer> orders: optimizedDBMatchingOrder) {
                    for(int o: orders) {
                        System.out.print(o + " ");
                    }
                    System.out.println();
                }
            }

            if(InstructionNotation.DEBUG) System.out.println("optimizedDBMatchingOrder.size: " + optimizedDBMatchingOrder.size());
            if(InstructionNotation.DEBUG) System.out.println("estimate partial pattern times: " + costEstimateCounter);

            totalOptimizedDBMatchingOrderSize += optimizedDBMatchingOrder.size();
            totalCostEstimateCounter += costEstimateCounter;

            long endTime = System.currentTimeMillis();
            if(InstructionNotation.DEBUG) System.out.println("Generate all matching orders with db optimized elapsed: " + (endTime - startTime)  + "ms");


            startTime = System.currentTimeMillis();

            // for all matching orders with optimized db costs
            for(List<Integer> orders: optimizedDBMatchingOrder) {
                // 2. generate raw execution plan
                List<ExecutionInstruction> execPlan = generateRawPlanFromMatchingOrder(orders);
                if(InstructionNotation.DEBUG) {
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
                //cacheTriangle(execPlan);

                //addFilteringCondition(execPlan);
                optimizedComputationalExecPlan.add(execPlan);

            }
            // 4. select the optimal execution plan with least db costs and computational costs
            selectOptimalExecPlan();

            endTime = System.currentTimeMillis();
            if(InstructionNotation.DEBUG) System.out.println("Computational cost optimized elapsed: " + (endTime - startTime)  + "ms");
        }
    }

    private void enumerateDynamicOrder(int vid, int index, List<Integer> vertexList, List<Integer> matchedVertex,
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
            }
        }

        // match vid
        matchedVertex.add(vid);

        if (matchedVertex.size() >= pattern.getVertexNum()) {
            // all pattern vertices have been matched
            BigInteger totalCost = estimatedCost.get(estimatedCost.size() - 1);
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

            List<Integer> matchedAdj = pattern.getInAndOutAdj(vid);
            matchedAdj.retainAll(matchedVertex);

            if(matchedAdj.size() < pattern.getInAndOutDegree(vid)) { // Case 1
                // estimate database cost
                PartialPatternMatchesEstimator estimator = new PartialPatternMatchesEstimator(partialPattern);
                BigInteger cost = estimator.computeCost(degArray, sumOfPower, matchedVertex.get(0), dataGraphVertexNum);

                costEstimateCounter++;

                totalCost = estimatedCost.get(estimatedCost.size() - 1).add(cost);

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

        // for disconnected partial pattern graph
        for (int j = 0; j < vertexList.size(); j++) {
            if (!matchedVertex.contains(vertexList.get(j))) {
                enumerateDynamicOrder(vertexList.get(j), j, vertexList, matchedVertex, partialPattern, estimatedCost,
                        equivalentVertex);
            }
        }

        matchedVertex.remove(matchedVertex.size() - 1);
        estimatedCost.remove(estimatedCost.size() - 1);

        partialPattern.removeVertex(vid);
    }

    private void updatePartialPattern(int vid, List<Integer> matchedVertex, Graph partialPattern) {
        // add edges between the last matched vertex and all its neighbors who have been matched.
        partialPattern.addVertex(vid);
        for(int x: matchedVertex) {
            if(pattern.getInAndOutAdj(vid).contains(x)) {
                partialPattern.addEdge(x, vid);
            }
        }
    }

    private List<ExecutionInstruction> generateRawPlanFromMatchingOrder(List<Integer> orders) {
        List<ExecutionInstruction> execPlan = new ArrayList<>();
        Set<Integer> fSet = new HashSet<Integer>();

                int src = orders.get(0);
                int dst = orders.get(1);

                // generate instructions for the first matching vertex : fx:=Init(start)
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INI,
                        InstructionNotation.ENUTARGET + src, ""));
                // add delta forward adj instruction: ADFx:=GetAdj(fx,delta,forward)
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                        InstructionNotation.DBQ_DELTA_FORWARD_TARGET + src, InstructionNotation.ENUTARGET + src));
                fSet.add(src);

                //C2:=Intersect(ADF1)
                //op,f2:=Foreach(C2)
                // automorphism conds
                List<Integer> condition = pattern.getCondition(dst); // symmetryBreakingConditions.get(id);
                List<Integer> automorphismConds = new ArrayList<>();
                if (condition != null) {
                    for (int c : condition) {
                        if (fSet.contains(Math.abs(c))) {
                            automorphismConds.add(c);
                        }
                    }
                }
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        InstructionNotation.INTCANDIDATE + dst,
                        InstructionNotation.DBQ_DELTA_FORWARD_TARGET + orders.get(0),
                        automorphismConds,
                        new ArrayList<>()));
                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DELTA_ENU,
                        InstructionNotation.ENUTARGET + dst, InstructionNotation.INTCANDIDATE + dst));
                fSet.add(dst);

                for (int i = 0; i < orders.size(); i++) {
                    int id = orders.get(i);
                    if (i > 1) {
                        // automorphism conds
                        condition = pattern.getCondition(id); // symmetryBreakingConditions.get(id);
                        automorphismConds = new ArrayList<>();
                        if (condition != null) {
                            for (int c : condition) {
                                if (fSet.contains(Math.abs(c))) {
                                    automorphismConds.add(c);
                                }
                            }
                        }

                        // injective conds
                        List<Integer> injectiveConds;
                        if (InstructionNotation.INJECTIVE_FILTER) {
                            injectiveConds = new ArrayList<>(fSet);
                            injectiveConds.removeAll(pattern.getInAndOutAdj(id));
                            injectiveConds.removeAll(automorphismConds);
                        } else {
                            injectiveConds = new ArrayList<>();
                        }

                        List<Integer> operands = new ArrayList<>(fSet);
                        List<Integer> inAdjs = pattern.getInAdj(id);
                        List<Integer> outAdjs = pattern.getOutAdj(id);
                        inAdjs.retainAll(fSet);
                        outAdjs.retainAll(fSet);

                        if (inAdjs.isEmpty() && outAdjs.isEmpty()) {
                            // fi is disconnected
                            execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                    InstructionNotation.INTCANDIDATE + id,
                                    InstructionNotation.DATAVERTEXSET,
                                    automorphismConds,
                                    injectiveConds));
                        } else if (inAdjs.size() + outAdjs.size() == 1) {
                            // fi=Ax
                            if (inAdjs.size() == 1) {
                                // fi = AFx
                                String operatorPrefix = isUnalteredDBQ(src, dst, inAdjs.get(0), id) ?
                                        InstructionNotation.DBQ_UNALTERED_FORWARD_TARGET : InstructionNotation.DBQ_EITHER_FORWARD_TARGET;
                                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                        InstructionNotation.INTCANDIDATE + id,
                                        operatorPrefix + inAdjs.get(0),
                                        automorphismConds,
                                        injectiveConds
                                        /*-1*/));
                            } else {
                                // fi = ARx
                                String operatorPrefix = isUnalteredDBQ(src, dst, id, outAdjs.get(0)) ?
                                        InstructionNotation.DBQ_UNALTERED_REVERSE_TARGET : InstructionNotation.DBQ_EITHER_REVERSE_TARGET;
                                execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                        InstructionNotation.INTCANDIDATE + id,
                                        operatorPrefix + outAdjs.get(0),
                                        automorphismConds,
                                        injectiveConds
                                        /*-1*/));
                            }

                        } else {
                            // fi=Ax,Ay,...
                            List<String> ops = new ArrayList<>();
                            String operatorPrefix;
                            for (Integer op : inAdjs) {
                                operatorPrefix = isUnalteredDBQ(src, dst, op, id) ?
                                        InstructionNotation.DBQ_UNALTERED_FORWARD_TARGET : InstructionNotation.DBQ_EITHER_FORWARD_TARGET;
                                ops.add(operatorPrefix + op);
                            }
                            for (Integer op : outAdjs) {
                                operatorPrefix = isUnalteredDBQ(src, dst, id, op) ?
                                        InstructionNotation.DBQ_UNALTERED_REVERSE_TARGET : InstructionNotation.DBQ_EITHER_REVERSE_TARGET;
                                ops.add(operatorPrefix + op);
                            }
                            execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                    InstructionNotation.INTTARGET + id, ops));

                            execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                    InstructionNotation.INTCANDIDATE + id,
                                    InstructionNotation.INTTARGET + id,
                                    automorphismConds,
                                    injectiveConds
                                    /*-1*/));

                        }
                        execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.ENU,
                                InstructionNotation.ENUTARGET + id, InstructionNotation.INTCANDIDATE + id));
                    }

                    execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                            InstructionNotation.DBQ_UNALTERED_FORWARD_TARGET + id, InstructionNotation.ENUTARGET + id));
                    execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                            InstructionNotation.DBQ_UNALTERED_REVERSE_TARGET + id, InstructionNotation.ENUTARGET + id));
                    execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                            InstructionNotation.DBQ_EITHER_FORWARD_TARGET + id, InstructionNotation.ENUTARGET + id));
                    execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.DBQ,
                            InstructionNotation.DBQ_EITHER_REVERSE_TARGET + id, InstructionNotation.ENUTARGET + id));
                    fSet.add(id);

                    if (i == 1) {
                        // check the existence of (f_k2, f_k1)
                        if (pattern.getOutAdj(id).contains(src)) {
                            // add INS instruction InSetTest(f_k1,A?O_k2)
                            List<String> ops = new ArrayList<>();
                            ops.add(InstructionNotation.ENUTARGET + src);
                            String operatorPrefix = isUnalteredDBQ(src, dst, id, src) ?
                                    InstructionNotation.DBQ_UNALTERED_FORWARD_TARGET : InstructionNotation.DBQ_EITHER_FORWARD_TARGET;
                            ops.add(operatorPrefix + id);
                            execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.INS,
                                    "", ops));
                        }
                    }
                }

        List<String> embedding = new ArrayList<String>();
        for (Integer fid : fSet) {
            embedding.add(InstructionNotation.ENUTARGET + fid);
        }
        execPlan.add(new ExecutionInstruction(ExecutionInstruction.InstructionType.RES,
                "f", embedding));

        computeExecutionPlanDependencies(execPlan);
        removeUnusedDBQInstructions(execPlan);
        eliminationUniOperand(execPlan);

        return execPlan;
    }

    private boolean isUnalteredDBQ(int deltaSrc, int deltaDst, int curSrc, int curDst) {
        // either,...,either,delta,unaltered,...unaltered
        boolean isUnalteredDBQ = false;
        for(int[] order : patternEdgeOrder) {
            if((order[0] == curSrc && order[1] == curDst)) {
                break;
            }
            if(order[0] == deltaSrc && order[1] == deltaDst) {
                isUnalteredDBQ = true;
            }
        }
        return isUnalteredDBQ;
    }

    /**
     * Return the type of edge(curSrc, curDst)
     * @param deltaSrc
     * @param deltaDst
     * @param curSrc
     * @param curDst
     * @return the type of edge, 0 is new, 1 is delta, 2 is old
     * @throws Exception
     */
    private int getPatternEdgeType(int deltaSrc, int deltaDst, int curSrc, int curDst) throws Exception {
        // either,...,either,delta,unaltered,...unaltered
        if(curSrc == deltaSrc && curDst == deltaDst) {
            return 1; // delta
        }
        boolean deltaFlag = false;
        for(int[] order : patternEdgeOrder) {
            if((order[0] == curSrc && order[1] == curDst)) {
                if(!deltaFlag) return 0; // either
                else return 2; // unaltered
            }
            if(order[0] == deltaSrc && order[1] == deltaDst) {
                deltaFlag = true;
            }
        }
        throw new Exception("The edge(curSrc, curDst) does not exist!");
    }

    private void computeExecutionPlanDependencies(List<ExecutionInstruction> execPlan) {
        Map<String, HashSet<String>> dependRecord = new HashMap<String, HashSet<String>>();
        for (ExecutionInstruction instruction : execPlan) {
            HashSet<String> dependSet = new HashSet<String>();
            if (instruction.isSingleOperator()) {
                String op = instruction.getSingleOperator();
                if (!op.equals(InstructionNotation.DATAVERTEXSET)) {
                    dependSet.add(op);
                }
                if (dependRecord.containsKey(op)) {
                    dependSet.addAll(dependRecord.get(op));
                }
                if (!instruction.getAutomorphismBreakingConds().isEmpty()) {
                    for (Integer cond : instruction.getAutomorphismBreakingConds()) {
                        op = InstructionNotation.ENUTARGET + Math.abs(cond);
                        dependSet.add(op);
                        if (dependRecord.containsKey(op)) {
                            dependSet.addAll(dependRecord.get(op));
                        }
                    }
                }
                if (!instruction.getInjectiveConds().isEmpty()) {
                    for (Integer cond : instruction.getInjectiveConds()) {
                        op = InstructionNotation.ENUTARGET + cond;
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

            String deltaForwardTarget = "";
            // the first and second instruction is INI and DELTA DBQ
            for (ExecutionInstruction instruction : execPlan) {
                if(instruction.getType() == ExecutionInstruction.InstructionType.DELTA_ENU) {
                    // the first ENU instruction is the second matched vertex: Cx:=Intersect(ADF1) op,fx:=Foreach(Cx)
                    deltaForwardTarget = instruction.getTargetVariable();
                    break;
                }
            }
            //System.out.println("deltaForwardTarget: " + deltaForwardTarget);
            HashSet<String> dfTargetDependencies = dependRecord.get(deltaForwardTarget);
            dfTargetDependencies.add(deltaForwardTarget);
            for (ExecutionInstruction instruction : execPlan) {
                if(!dfTargetDependencies.contains(instruction.getTargetVariable())) {
                    // add dfTargetDependencies to the dependencies of instruction
                    HashSet<String> newDependencies = new HashSet<>();
                    newDependencies.addAll(instruction.getDependOn());
                    newDependencies.addAll(dfTargetDependencies);
                    instruction.setDependOn(new ArrayList<>(newDependencies));
                }
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
                        key.add(InstructionNotation.INTTARGET + Ti);
                        key.removeAll(maxFrequentSet);
                        if (flag) {
                            List<String> operators = new ArrayList<>(maxFrequentSet);
                            operators.sort((a,b) -> INTOperatorsPos.get(a).compareTo(INTOperatorsPos.get(b)));

                            ExecutionInstruction newInstruction = new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                                    InstructionNotation.INTTARGET + Ti, operators);
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
        eliminationUniOperand(execPlan);

        if (InstructionNotation.DEBUG) {
            System.out.println("**************eliminate***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private void eliminationUniOperand(List<ExecutionInstruction> execPlan) {
        Iterator listIter = execPlan.iterator();
        Map<String, String> record = new HashMap<String, String>();
        while (listIter.hasNext()) {
            ExecutionInstruction instruction = (ExecutionInstruction) listIter.next();
            if (instruction.getType() != ExecutionInstruction.InstructionType.INI &&
                    instruction.getType() != ExecutionInstruction.InstructionType.RES) {
                if (instruction.getType() == ExecutionInstruction.InstructionType.INT &&
                        instruction.isSingleOperator() &&
                        instruction.getInjectiveConds().isEmpty() &&
                        instruction.getAutomorphismBreakingConds().isEmpty()) {
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
                operators.add(0, InstructionNotation.INTTARGET + Ti);
                ExecutionInstruction newinstruction = new ExecutionInstruction(ExecutionInstruction.InstructionType.INT,
                        InstructionNotation.INTTARGET + Ti, newOperators);
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
        System.out.println("**************computeExecutionPlanDependencies***************");
        for (ExecutionInstruction instruction : execPlan) {
            System.out.print(instruction);
            System.out.print(" | ");
            System.out.println(instruction.getDependOn());
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

        if (InstructionNotation.DEBUG) {
            System.out.println("**************reorder***************");
            for (ExecutionInstruction instruction : execPlan) {
                System.out.println(instruction);
            }
        }
    }

    private void selectOptimalExecPlan() {
        String optimal = "";
        //long leastCost = Long.MAX_VALUE;
        BigInteger leastCost = BigInteger.valueOf(-1);
        int leastCostPlanDepth = Integer.MAX_VALUE;

        for(int i = 0; i < optimizedComputationalExecPlan.size(); i++) {
            List<ExecutionInstruction> execPlan = optimizedComputationalExecPlan.get(i);

            BigInteger cost = getTotalComputationalCost(execPlan);

            int depth = getPlanDepth(execPlan);

            if (InstructionNotation.DEBUG) {
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

        if(InstructionNotation.DEBUG) System.out.println("the number of optimal execution plan: " + optimal.split(" ").length);

        if (InstructionNotation.DEBUG) {
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

            System.out.println();
        }
    }

    public BigInteger getTotalComputationalCost(List<ExecutionInstruction> execPlan) {
        Graph partialPattern = new Graph();
        BigInteger totalCost = BigInteger.ZERO;
        BigInteger partialMatchesNum = BigInteger.ZERO;
        int startVid = -1;
        for(ExecutionInstruction instruction: execPlan) {
            if(instruction.getType() == ExecutionInstruction.InstructionType.INI) {
                // fx=Init(start)
                String target = instruction.getTargetVariable();
                int v = Integer.parseInt(target.substring(InstructionNotation.ENUTARGET.length()));
                partialPattern.addVertex(v);
                partialMatchesNum = BigInteger.valueOf(dataGraphVertexNum);
                startVid = v;
            }
            else if(instruction.getType() == ExecutionInstruction.InstructionType.ENU ||
                    instruction.getType() == ExecutionInstruction.InstructionType.DELTA_ENU ) {
                // fx=Foreach(Cx)
                String target = instruction.getTargetVariable();
                int v = Integer.parseInt(target.substring(InstructionNotation.ENUTARGET.length()));
                updatePartialPattern(v, partialPattern.getAllVertices(), partialPattern);
                PartialPatternMatchesEstimator estimator = new PartialPatternMatchesEstimator(partialPattern);
                partialMatchesNum = estimator.computeCost(degArray, sumOfPower, startVid, dataGraphVertexNum);
                //partialPattern.print();
                //System.out.println("partialMatchesNum: " + partialMatchesNum);
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
                int v = Integer.parseInt(target.substring(InstructionNotation.ENUTARGET.length()));
                partialPattern.addVertex(v);
                startVid = v;
            }
            else if(instruction.getType() == ExecutionInstruction.InstructionType.DBQ) {
                String target = instruction.getTargetVariable();
                int prefixLength = computeDBQTargetPrefixLength(target);
                int v = Integer.parseInt(target.substring(prefixLength));
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

    private int computeDBQTargetPrefixLength(String target) {
        int length;
        if (target.startsWith(InstructionNotation.DBQ_DELTA_FORWARD_TARGET) ||
                target.startsWith(InstructionNotation.DBQ_EITHER_FORWARD_TARGET) ||
                target.startsWith(InstructionNotation.DBQ_EITHER_REVERSE_TARGET) ||
                target.startsWith(InstructionNotation.DBQ_UNALTERED_FORWARD_TARGET) ||
                target.startsWith(InstructionNotation.DBQ_UNALTERED_REVERSE_TARGET))
            length = InstructionNotation.DBQ_DELTA_FORWARD_TARGET.length();
        else length = 1;
        return length;
    }

    private void refineFilterCondition(List<ExecutionInstruction> execPlan) {
        // (1) >n, ≠n => >n；<n, ≠n => <n
        // (2) >n1, >n2 and n1 > n2 or n1 < n2 => >max(n1, n2)
        // (3) <n1, <n2 and n1 > n2 or n1 < n2 => <min(n1, n2)
        Map<Integer, List<Integer>> condsRecodes = new HashMap<>(); // store f and the automorphism breaking conditions
        for(ExecutionInstruction instruction: execPlan) {
            if(instruction.getType() == ExecutionInstruction.InstructionType.INT) {
                // C/T = Intersect(X,Y)
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
                            injectiveCondes.remove(new Integer(c));
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
                        }
                    }
                    instruction.setAutomorphismBreakingConds(newAutoBreakingConds);
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
                int degree = line.split(InstructionNotation.DATAGRAPH_SEPARATOR).length - 1;
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
}
