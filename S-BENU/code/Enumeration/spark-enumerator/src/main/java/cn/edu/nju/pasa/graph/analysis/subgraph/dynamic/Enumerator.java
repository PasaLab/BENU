package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.storage.multiversion.AdjType;
import cn.edu.nju.pasa.graph.storage.multiversion.GraphWithTimestampDBClient;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.*;

/**
 * The Enumerator for a execution plan.
 * Created by huweiwei on 5/12/2019.
 */
public final class Enumerator implements Serializable {
    private int instructionsNum;
    private int patternVertexNum;
    private int[] type; // types of execution instructions: 0(INI), 1(DBQ), 2(INT), 3(ENU), 4(RES), 5(INS)
    //private int[] INI; // 0:init(start)
    //private AdjType[] adjType;
    private int[] DBQ; // operatorID
    private AdjType[] DBQType; // 0(either), 1(delta), 2(unaltered)
    private AdjType[] DBQDirection; // 0(forward), 1(reverse)
    private int[][] INT; // operator1ID, operator2ID, ...
    private int[] ENU; // OpertorID
    private int[] lastForLoopIndex;
    private int[] matchedVertexCounter;
    private int[][] patternVertexToENUMap; // {{pattern vertex id1, ENU id}, {pattern vertex id2, ENU id}, ...}
    private int[][] injectiveConds;
    private int[][] automorphismConds;
    private int[][] INS; // operator1ID(fx), operator2ID(A?Oy)

    private GraphWithTimestampDBClient graphStorage;
    private int timestamp;
    private IntArrayList[] result;

    private IntArrayList filterVertices;

    /**
     * Init the enumeration.
     * @param N number of instructions
     */
    public Enumerator(int N) {
        instructionsNum = N;
        type = new int[N];
        //INI = new int[N];
        //adjType = new AdjType[N];
        DBQ = new int[N];
        DBQType = new AdjType[N];
        DBQDirection = new AdjType[N];
        INT = new int[N][N];
        ENU = new int[N];
        lastForLoopIndex = new int[N];
        matchedVertexCounter = new int[N];
        patternVertexToENUMap = new int[N][2];
        injectiveConds = new int[N][];
        automorphismConds = new int[N][];
        INS = new int[N][2];
    }

    public Enumerator(Enumerator enumerator) {
        instructionsNum = enumerator.getInstructionsNum();
        patternVertexNum = enumerator.getPatternVertexNum();
        type = enumerator.getType();
        //INI = enumerator.getINI();
        //adjType = enumerator.getAdjType();
        DBQ = enumerator.getDBQ();
        DBQType = enumerator.getDBQType();
        DBQDirection = enumerator.getDBQDirection();
        INT = enumerator.getINT();
        ENU = enumerator.getENU();
        lastForLoopIndex = enumerator.getLastForLoopIndex();
        matchedVertexCounter = enumerator.getMatchedVertexCounter();
        patternVertexToENUMap = enumerator.getPatternVertexToENUMap();
        injectiveConds = enumerator.getInjectiveConds();
        automorphismConds = enumerator.getAutomorphismConds();
        graphStorage = enumerator.getGraphStorage();
        timestamp = enumerator.getTimestamp();
        result = enumerator.getResult();
        filterVertices = enumerator.getFilterVertices();
        INS = enumerator.getINS();
    }

    public int[] getType() {
        return type;
    }

    /*
    public int[] getINI() {
        return INI;
    }

     */

    /*
    public AdjType[] getAdjType() {
        return adjType;
    }
     */

    public int[] getDBQ() { return DBQ; }

    public AdjType[] getDBQType() { return DBQType; }

    public AdjType[] getDBQDirection() { return DBQDirection; }

    public int[][] getINT() { return INT; }

    public int[] getENU() { return ENU; }

    public int[] getLastForLoopIndex() { return lastForLoopIndex; }

    public int[] getMatchedVertexCounter() { return matchedVertexCounter; }

    public int[][] getPatternVertexToENUMap() {return patternVertexToENUMap; }

    public int[][] getInjectiveConds() { return injectiveConds; }

    public int[][] getAutomorphismConds() { return automorphismConds; }

    public int getInstructionsNum() {
        return instructionsNum;
    }

    public int getPatternVertexNum() {return patternVertexNum; }

    public GraphWithTimestampDBClient getGraphStorage() {
        return graphStorage;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public IntArrayList[] getResult() {
        return result;
    }

    public IntArrayList getFilterVertices() { return filterVertices; }

    public int[][] getINS() {return INS;}

    /**
     * Construct the enumerator
     * @param execPlan
     * @throws Exception
     */
    public void construct(String[] execPlan) throws Exception {
        Map<String, Integer> targetID = new HashMap<>();

        int forLoopIndex = -1;
        int matchedVertexNum = 0;

        System.out.print("order: ");
        for(int i = 0; i < execPlan.length; i++) {
            // target:=Operation(Operand1,Operand2,...)
            String target;
            String[] instruction;
            String operation;
            String[] operands;
            if(execPlan[i].startsWith(InstructionNotation.INS)) {
                target = "";
                instruction = execPlan[i].split("\\(");
                operation = instruction[0];
                int opEndIndex = instruction[1].indexOf(")");
                operands = instruction[1].substring(0, opEndIndex).split(",");
            } else {
                String[] fields = execPlan[i].split(":=");
                String[] targets = fields[0].split(",");
                if (targets.length > 1) target = targets[1]; // op,fx return fx
                else target = targets[0];
                instruction = fields[1].split("\\(");
                operation = instruction[0];
                int opEndIndex = instruction[1].indexOf(")");
                operands = instruction[1].substring(0, opEndIndex).split(",");
                int filterSeperatorIndex = instruction[1].indexOf("|");
                if (filterSeperatorIndex >= 0) {
                    String[] filterConds = instruction[1].substring(filterSeperatorIndex + 2).split(",");
                    IntArrayList injective = new IntArrayList();
                    IntArrayList automorphism = new IntArrayList();
                    List<Integer> x = new ArrayList<>();
                    for (String cond : filterConds) {
                        if (cond.startsWith("!")) {
                            String id = cond.substring(1);
                            injective.add((int) targetID.get(id));
                        } else if (cond.startsWith(">")) {
                            String id = cond.substring(1);
                            automorphism.add((int) targetID.get(id));
                        } else if (cond.startsWith("<")) {
                            String id = cond.substring(1);
                            automorphism.add(CommonFunctions.setSignBitTo1(targetID.get(id)));
                        } else {
                            throw new Exception("Illegal instruction, unrecognized filter condition: " + execPlan[i]);
                        }
                    }
                    injectiveConds[i] = injective.toIntArray();
                    automorphismConds[i] = automorphism.toIntArray();
                }

                targetID.put(target, i);
            }
            /*
            System.err.println("target: " + target + ", id: " + (i+1));
            if(injectiveConds[i] != null) {
                System.err.println("injective: " + Arrays.toString(injectiveConds[i]));
            }
            if(automorphismConds[i] != null) {
                System.err.println("automorphism: " + Arrays.toString(automorphismConds[i]));
            }
             */

            lastForLoopIndex[i] = forLoopIndex;
            matchedVertexCounter[i] = matchedVertexNum;

            switch (operation) {
                case InstructionNotation.INIT:
                    if(operands[0].equals(InstructionNotation.INIT_START)) {
                        // fx:=Init(start)
                        type[i] = 0;
                        //INI[i] = 0;
                    } else {
                        // fx:=Init(dst)
                        type[i] = 0; //INI[i] = 1;
                    }
                    patternVertexToENUMap[matchedVertexNum][0] = Integer.parseInt(target.substring(1)) ;// pattern vertex id;
                    patternVertexToENUMap[matchedVertexNum][1] = i ;// enumeratoion instruction id;
                    matchedVertexNum++; matchedVertexCounter[i] = matchedVertexNum;
                    System.out.print(target + ",");
                    break;
                case InstructionNotation.DBQ:
                    // Ax = GetAdj(fx, type, forward/reverse, op)
                    type[i] = 1; DBQ[i] = targetID.get(operands[0]);

                    // store dbq type: either(0), delta(1), unaltered(2)
                    String typeOp = operands[1];
                    AdjType dbqtype;
                    if(typeOp.equalsIgnoreCase(InstructionNotation.DBQ_TYPE_EITHER)) {
                        dbqtype = AdjType.EITHER;
                    } else if(typeOp.equalsIgnoreCase(InstructionNotation.DBQ_TYPE_DELTA)) {
                        dbqtype = AdjType.DELTA;
                    } else if(typeOp.equalsIgnoreCase(InstructionNotation.DBQ_TYPE_UNALTERED)) {
                        dbqtype = AdjType.UNALTERED;
                    } else throw new Exception("Illegal instruction, unrecognized adj type: " + execPlan[i]);
                    DBQType[i] = dbqtype;

                    // store dbq direction: forward(0), reverse(1)
                    String directionOp = operands[2];
                    AdjType direction;
                    if(directionOp.equalsIgnoreCase(InstructionNotation.DBQ_FORWARD)) {
                        direction = AdjType.FORWARD;
                    } else if(directionOp.equalsIgnoreCase(InstructionNotation.DBQ_REVERSE)) {
                        direction = AdjType.REVERSE;
                    }  else throw new Exception("Illegal instruction, nrecognized adj direction: " + execPlan[i]);
                    DBQDirection[i] = direction;
                    break;
                case InstructionNotation.SET_INTERSECTION:
                    type[i] = 2;
                    for(int j = 0; j < operands.length; j++) {
                        INT[i][j] = targetID.get(operands[j]);
                    }
                    break;
                case InstructionNotation.ENUMERATION:
                    type[i] = 3; ENU[i] = targetID.get(operands[0]); forLoopIndex = i;
                    patternVertexToENUMap[matchedVertexNum][0] = Integer.parseInt(target.substring(1)) ;// pattern vertex id;
                    patternVertexToENUMap[matchedVertexNum][1] = i ;// enumeratoion instruction id;
                    matchedVertexNum++; matchedVertexCounter[i] = matchedVertexNum;
                    System.out.print(target + ",");
                    break;
                case InstructionNotation.RESULT_REPORTING:
                    type[i] = 4;
                    System.out.print("\n");
                    break;
                case InstructionNotation.INS:
                    type[i] = 5;
                    INS[i][0] = targetID.get(operands[0]);
                    INS[i][1] = targetID.get(operands[1]);
                    break;
                default:
                    throw new Exception("Illegal instruction: " + execPlan[i]);
            }
        }

        patternVertexNum = matchedVertexNum;
        Arrays.sort(patternVertexToENUMap, 0, patternVertexNum, (a, b) -> a[0] - b[0]);
    }

    public final void prepare(Properties conf) {
        try {
            graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();
            timestamp = Integer.parseInt(conf.getProperty(MyConf.TIMESTAMP, "0"));

            result = new IntArrayList[instructionsNum]; // result[i] is the computation result for i-th instruction
            for(int i = 0; i < instructionsNum; i++) {
                result[i] = new IntArrayList();
            }

            filterVertices = new IntArrayList();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public final VertexTaskStats executeVertexTask(int vid, VertexCentricAnalysisTask task, MatchesCollectorInterface collector) throws Exception {
        //System.err.println("[Enumerator] executeVertexTask...");
        long localCount = 0L;
        long dbqCount = 0L;
        long intersectionCount = 0L;
        long dbQueryingTime = 0L;
        long enuCount = 0L;
        long t0 = System.nanoTime();
        /*
        IntArrayList[] result = new IntArrayList[instructionsNum];
        for(int i = 0; i < instructionsNum; i++) {
            result[i] = new IntArrayList();
        }
        */
        //String enu = "";

        // If op == '-', deleted is TRUE; If op == '+', deleted is FALSE
        boolean deleted = false;

        // temp result set for INT instruction
        IntArrayList tmp = new IntArrayList();

        for(int i = 0; i < instructionsNum; ) {
            int t = type[i];
            int id = -1;
            IntArrayList X; // X points to the target variable of the instruction
            //System.err.println("type: " + t);
            switch (t) {
                case 0:
                    // INI
                    int fx = vid;
                    X = result[i];
                    X.clear();
                    X.add(fx);
                    i++;
                    break;
                case 1:
                    // DBQ
                    if(DBQType[i] == AdjType.DELTA) {
                        // Get delta forward adj.
                        X = result[i];
                        X.clear();
                        int taskA1[] = Arrays.copyOfRange(task.getAdj(), task.getStart(), task.getEnd() + 1);
                        X.addElements(0, taskA1);
                        i++;
                    } else {
                        // Get old or new adj.
                        dbqCount++;
                        id = DBQ[i];
                        long tstart = System.nanoTime();
                        int[] Ax = graphStorage.get(result[id].getInt(0), DBQType[i], DBQDirection[i], !deleted, timestamp);
                        long tend = System.nanoTime();
                        dbQueryingTime += tend - tstart;
                        if (Ax.length < 1) {
                            i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                        } else {
                            X = result[i];
                            X.clear();
                            X.addElements(0, Ax);
                            i++;
                        }
                    }
                    /*
                    if(adjType[i] == AdjType.DELTA_FORWARD) {
                        //System.err.println("i: " + i + " Get delta forward adj.");
                        X = result[i];
                        X.clear();
                        X.addElements(0, task.getAdj());
                        i++;
                    } else {
                        //System.err.println("i: " + i + " Get old or new adj.");
                        id = DBQ[i];
                        int[] Ax = graphStorage.get(result[id].getInt(0), adjType[i], timestamp, !deleted);
                        if (Ax.length < 1) {
                            i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                        } else {
                            X = result[i];
                            X.clear();
                            X.addElements(0, Ax);
                            i++;
                        }
                    }
                     */
                    break;
                case 2:
                    // INT
                    intersectionCount++;
                    X = result[i];
                    X.clear();
                    int operand1 = INT[i][0];
                    int operand2 = INT[i][1];
                    /*
                    if(operand2 == 0) {
                        X.addAll(result[operand1]);
                        i++;
                    } else {
                        CommonFunctions.intersectTwoSortedArrayAdaptive(X, result[operand1], result[operand2]);
                        if(X.size() < 1)
                            i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                        else i++;
                    }

                     */

                    if(operand2 == 0) {
                        // there are only one operand. Cx:=Intersect(Tx) or Cx:=Intersect(Ax)
                        // X.addAll(result[operand1]);
                        // filter vertices which don't satisfy injective condition or automorphism condition
                        filterVertices.clear();
                        if(injectiveConds[i] != null) {
                            for (int injectiveId : injectiveConds[i]) {
                                filterVertices.add(result[injectiveId].getInt(0));
                            }
                        }
                        int min = -1;
                        int max = -1;
                        if(automorphismConds[i] != null) {
                            for (int autoId : automorphismConds[i]) {
                                if (!CommonFunctions.isSignBit1(autoId)) { // >autoId
                                    min = Math.max(min, result[autoId].getInt(0));
                                } else { // <autoId
                                    int temp = result[autoId].getInt(0);
                                    if(max < 0 || max > temp)
                                        max = temp;
                                }
                            }
                        }
                        // the value add to X must !=v in filterVertices, >min, <max
                        for(int value: result[operand1]) {
                            int v = CommonFunctions.setSignBitTo0(value);
                            if(filterVertices.contains(v)) continue;
                            if(v <= min) continue;
                            if(max >= 0 && v >= max) break;
                            X.add(value);
                        }
                        //i++;
                    } else {
                        // there are two or more operands. Tx:=Intersect(X, Y, ...)
                        CommonFunctions.intersectTwoSortedArrayAdaptive(X, result[operand1], result[operand2]);
                        for(int k = 2; k < INT[i].length; k++) {
                            if(INT[i][k] != 0) {
                                tmp.clear();
                                tmp.addAll(X);
                                int operand = INT[i][k];
                                X.clear();
                                CommonFunctions.intersectTwoSortedArrayAdaptive(X, tmp, result[operand]);
                                if(X.size() < 1) break;
                            } else {
                                break;
                            }
                        }
                    }
                    if(X.size() < 1)
                        i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                    else i++;

                    break;
                case 3:
                    // ENU
                    id = ENU[i]; // fx := Interset(Cy) id is OpertorID , i.e. Cy's Id

                    // save the current value for forloop and the index of value in the candidate set
                    X = result[i]; // result[i][0] is the value of fx, result[i][1] is the searched index in Cy's result
                    enuCount++;
                    if(X.isEmpty()) { // get the first value in candidate set
                        int value = result[id].getInt(0);
                        if(matchedVertexCounter[i] == 2) {
                            // if fx is the second vertex to match, so Cy = GETDELTAFORWARDADJ()
                            deleted = CommonFunctions.isSignBit1(value);
                        }
                        X.add(CommonFunctions.setSignBitTo0(value)); // the current value for forloop
                        X.add(0); // the index of value in the candidate set
                        i++;
                    } else {
                        int size = result[id].size();
                        int newIndex = X.getInt(1) + 1; // the new index is lastindex+1
                        if (newIndex >= size) { // finish traversing Cy
                            X.clear();
                            i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                        }
                        else {
                            int newValue = result[id].getInt(newIndex);
                            if(matchedVertexCounter[i] == 2) {
                                // if fx is the second vertex to match, so Cy = GETDELTAFORWARDADJ()
                                deleted = CommonFunctions.isSignBit1(newValue);
                            }
                            X.set(0, CommonFunctions.setSignBitTo0(newValue));
                            X.set(1, newIndex);
                            i++;
                        }
                    }
                    break;
                case 4:
                    // RES
                    localCount++;
                    int op = deleted ? -1 : 1;
                    int[] reportedMathes = new int[patternVertexNum + 1];
                    for(int j = 0; j < patternVertexNum; j++) {
                        int targetId = patternVertexToENUMap[j][1];
                        reportedMathes[j] = result[targetId].getInt(0);
                    }
                    reportedMathes[patternVertexNum] = op;
                    collector.offer(reportedMathes, op);
                    i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                    break;
                case 5:
                    // InSetTest(fId,dbqId)
                    int fId = INS[i][0];
                    int dbqId = INS[i][1];
                    int fValue = result[fId].getInt(0);
                    IntArrayList dbqValue = result[dbqId];
                    if(!dbqValue.contains(fValue))
                        i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                    else i++;
                    break;
                default:
                    throw new Exception("Illegal instruction type!");
            }
        }
        // clear the result.
        for(IntArrayList r : result) {
            r.clear();
        }

        long t1 = System.nanoTime();
        VertexTaskStats stats = new VertexTaskStats(vid, task.getAdj().length, task.getEnd()-task.getStart()+1,
                localCount, (t1 - t0), HostInfoUtil.getHostName(), dbqCount, intersectionCount, dbQueryingTime, 0);
        return stats;
    }

    public final VertexTaskStats executeVertexTask(int vid, VertexCentricAnalysisTask task, MatchesCollectorInterface collector,
                                                   int taskIndex, LongAccumulator[] accus) throws Exception {
        //System.err.println("[Enumerator] executeVertexTask...");
        long localCount = 0L;
        long t0 = System.nanoTime();
        //String enu = "";
        boolean deleted = false;
        for(int i = 0; i < instructionsNum; ) {
            int t = type[i];
            int id = -1;
            IntArrayList X;
            //System.err.println("type: " + t);
            switch (t) {
                case 0:
                    // INI
                    int fx = vid;
                    X = result[i];
                    X.clear();
                    X.add(fx);
                    i++;
                    accus[taskIndex * patternVertexNum + matchedVertexCounter[i] - 1].add(1L);
                    break;
                case 1:
                    // DBQ
                    if(DBQType[i] == AdjType.DELTA) {
                        // Get delta forward adj.
                        X = result[i];
                        X.clear();
                        X.addElements(0, task.getAdj());
                        i++;
                    } else {
                        // Get old or new adj.
                        id = DBQ[i];
                        int[] Ax = graphStorage.get(result[id].getInt(0), DBQType[i], DBQDirection[i], !deleted, timestamp);
                        /*
                        if (Ax.length < 1) {
                            i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                        } else {
                            X = result[i];
                            X.clear();
                            X.addElements(0, Ax);
                            i++;
                        }
                         */
                        X = result[i];
                        X.clear();
                        X.addElements(0, Ax);
                        i++;
                    }
                    break;
                case 2:
                    // INT
                    X = result[i];
                    X.clear();
                    int operand1 = INT[i][0];
                    int operand2 = INT[i][1];

                    if(operand2 == 0) {
                        X.addAll(result[operand1]);
                        i++;
                    } else {
                        CommonFunctions.intersectTwoSortedArrayAdaptive(X, result[operand1], result[operand2]);
                        /*
                        if(X.size() < 1)
                            i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                        else i++;

                         */
                        i++;
                    }

                    /*
                    if(operand2 == 0) {
                        // there are only one operand. Cx:=Intersect(Tx) or Cx:=Intersect(Ax)
                        // X.addAll(result[operand1]);
                        // filter vertices which don't satisfy injective condition and automorphism condition
                        filterVertices.clear();
                        if(injectiveConds[i] != null) {
                            for (int injectiveId : injectiveConds[i]) {
                                filterVertices.add(result[injectiveId].getInt(0));
                            }
                        }
                        int min = -1;
                        int max = -1;
                        if(automorphismConds[i] != null) {
                            for (int autoId : automorphismConds[i]) {
                                if (!CommonFunctions.isSignBit1(autoId)) { // >autoId
                                    min = Math.max(min, result[autoId].getInt(0));
                                } else { // <autoId
                                    int temp = result[autoId].getInt(0);
                                    if(max < 0 || max > temp)
                                        max = temp;
                                }
                            }
                        }
                        // the value add to X must !=v in filterVertices, >min, <max
                        for(int value: result[operand1]) {
                            int v = CommonFunctions.setSignBitTo0(value);
                            if(filterVertices.contains(v)) continue;
                            if(v <= min) continue;
                            if(max >= 0 && v >= max) break;
                            X.add(value);
                        }
                        //i++;
                    } else {
                        // there are two operands. Tx:=Intersect(X, Y)
                        CommonFunctions.intersectTwoSortedArrayAdaptive(X, result[operand1], result[operand2]);

                    }
                    if(X.size() < 1)
                        i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                    else i++;
                    */
                    break;
                case 3:
                    // ENU
                    id = ENU[i]; // fx := Interset(Cy) id is OpertorID , i.e. Cy's Id

                    if(result[id].size() < 1)
                        i = lastForLoopIndex[i] == -1 ? instructionsNum - 1 : lastForLoopIndex[i]; // backtrack
                    else {

                        // save the current value for forloop and the index of value in the candidate set
                        X = result[i]; // result[i][0] is the value of fx, result[i][1] is the traversed index in Cy's result
                        if (X.isEmpty()) { // get the first value in candidate set
                            int value = result[id].getInt(0);
                            if (matchedVertexCounter[i] == 2) {
                                // if fx is the second vertex to match, so Cy = GETDELTAFORWARDADJ()
                                deleted = CommonFunctions.isSignBit1(value);
                            }
                            X.add(CommonFunctions.setSignBitTo0(value)); // the current value for forloop
                            X.add(0); // the index of value in the candidate set
                            i++;
                            accus[taskIndex * patternVertexNum + matchedVertexCounter[i] - 1].add(1L);
                        } else {
                            int size = result[id].size();
                            int newIndex = X.getInt(1) + 1; // the new index is lastindex+1
                            if (newIndex >= size) { // finish traversing Cy
                                X.clear();
                                i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                            } else {
                                int newValue = result[id].getInt(newIndex);
                                if (matchedVertexCounter[i] == 2) {
                                    // if fx is the second vertex to match, so Cy = GETDELTAFORWARDADJ()
                                    deleted = CommonFunctions.isSignBit1(newValue);
                                }
                                X.set(0, CommonFunctions.setSignBitTo0(newValue));
                                X.set(1, newIndex);
                                i++;
                                accus[taskIndex * patternVertexNum + matchedVertexCounter[i] - 1].add(1L);
                            }
                        }
                    }
                    break;
                case 4:
                    localCount++;
                    int op = deleted ? -1 : 1;
                    int[] reportedMathes = new int[patternVertexNum + 1];
                    for(int j = 0; j < patternVertexNum; j++) {
                        int targetId = patternVertexToENUMap[j][1];
                        reportedMathes[j] = result[targetId].getInt(0);
                    }
                    reportedMathes[patternVertexNum] = op;
                    collector.offer(reportedMathes, op);
                    i = lastForLoopIndex[i] == -1 ? instructionsNum : lastForLoopIndex[i]; // backtrack
                    break;
                default:
                    throw new Exception("Illegal instruction type!");
            }
        }
        // clear the result.
        for(IntArrayList r : result) {
            r.clear();
        }

        long t1 = System.nanoTime();
        VertexTaskStats stats = new VertexTaskStats(vid, task.getAdj().length, -1,
                localCount, (t1 - t0), HostInfoUtil.getHostName());
        return stats;
    }

    private final int backtrack(int curPos) {
        // if no more for loops before current instruction, go to the end of the execution plan
        return lastForLoopIndex[curPos] == -1 ? instructionsNum - 1 : lastForLoopIndex[curPos];
    }

    public static Enumerator newInstance(Enumerator enumerator) {
        return new Enumerator(enumerator);
    }

}
