package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.storage.multiversion.AdjType;
import cn.edu.nju.pasa.graph.storage.multiversion.GraphWithTimestampDBClient;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Serializable;
import java.util.*;

/**
 * A local search subtask
 * The task of the enumerator is splitable. A task can be divided into several sub-tasks during the task execution.
 * Created by wzk on Dec 1, 2020.
 */
public final class LocalSearchSubTask implements Serializable {

    private static final long serialVersionUID = 42L;
    // task-dependent variables
    private int timestamp;
    private IntArrayList[] result;
    private final IncrementalExecutionPlan executionPlan;
    // task-related variables
    private int startInstructionIndex = 0;
    private int i = 0; // the current instruction index
    private int centerVertexID = 0;
    private LocalSearchTask centerVertexTask = null;
    private boolean deleted = false; // If op = '-', deleted = true;
    private boolean enableBalance = false;
    private int balanceThresholdIntraNode[] = {5, 50, 500};
    private int balanceThresholdInterNode[] = {5,50,500};
    private int maxBalanceDepth = 3;
    public boolean isHeavyTask = false;
    private int numSplitInternode = 1;

    private int[] parseIntArrayString(String str) {
        String elements[] = str.split(",");
        int arr[] = new int[elements.length];
        for (int j = 0; j < elements.length; j++) {
            arr[j] = Integer.parseInt(elements[j]);
        }
        return arr;
    }

    /**
     * Init the enumeration.
     * @param plan incremental execution plan
     * @param conf configuration
     */
    public LocalSearchSubTask(IncrementalExecutionPlan plan, Properties conf) throws Exception {
        this.executionPlan = plan;
        timestamp = Integer.parseInt(conf.getProperty(MyConf.TIMESTAMP, "0"));
        enableBalance = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_LOAD_BALANCE, "false"));
        if (enableBalance) {
            String intraNodeBalanceConf = conf.getProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTRANODE, "5,50,500");
            balanceThresholdIntraNode = parseIntArrayString(intraNodeBalanceConf);
            String interNodeBalanceConf = conf.getProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTERNODE, "5,50,500");
            balanceThresholdInterNode = parseIntArrayString(interNodeBalanceConf);
            maxBalanceDepth = Integer.parseInt(conf.getProperty(MyConf.MAX_BALANCE_DEPTH, MyConf.Default.MAX_BALANCE_DEPTH));
        }
        numSplitInternode = Integer.parseInt(conf.getProperty(MyConf.NUM_SPLIT_INTERNODE, "16"));
        result = new IntArrayList[executionPlan.instructionsNum]; // result[i] is the computation result for i-th instruction
        for (int i = 0; i < result.length; i++) {
            result[i] = new IntArrayList();
        }
    }

    /**
     * Create a local search sub-task based another sub-task.
     * The result arrays are cloned. The read-only member objects are re-used.
     * @param anotherTask the task to copy
     */
    public LocalSearchSubTask(LocalSearchSubTask anotherTask) {
        this.timestamp = anotherTask.timestamp;
        this.result = new IntArrayList[anotherTask.result.length];
        for (int j = 0; j < result.length; j++) {
            result[j] = anotherTask.result[j].clone();
        }
        this.executionPlan = anotherTask.executionPlan;
        this.startInstructionIndex = anotherTask.startInstructionIndex;
        this.i = anotherTask.i;
        this.centerVertexID = anotherTask.centerVertexID;
        this.centerVertexTask = anotherTask.centerVertexTask;
        this.deleted = anotherTask.deleted;
        this.enableBalance = anotherTask.enableBalance;
        this.balanceThresholdIntraNode = anotherTask.balanceThresholdIntraNode;
        this.maxBalanceDepth = anotherTask.maxBalanceDepth;
        this.isHeavyTask = anotherTask.isHeavyTask;
        this.numSplitInternode = anotherTask.numSplitInternode;
    }

    /**
     * Create another local search subtask from the current local search subtask.
     * The result sets before `startInstructionIndex` are re-used.
     * The result sets after `startInstructionIndex` are newly created.
     * @param anotherTask
     * @param startInstructionIndex
     */
    public LocalSearchSubTask(LocalSearchSubTask anotherTask, int startInstructionIndex) {
        this.timestamp = anotherTask.timestamp;
        this.result = new IntArrayList[anotherTask.result.length];
        for (int j = 0; j < startInstructionIndex; j++) {
            result[j] = anotherTask.result[j];
        }
        for (int j = startInstructionIndex; j < result.length; j++) {
            result[j] = new IntArrayList();
        }
        this.executionPlan = anotherTask.executionPlan;
        this.startInstructionIndex = startInstructionIndex;
        this.i = anotherTask.i;
        this.centerVertexID = anotherTask.centerVertexID;
        this.centerVertexTask = anotherTask.centerVertexTask;
        this.deleted = anotherTask.deleted;
        this.enableBalance = anotherTask.enableBalance;
        this.balanceThresholdIntraNode = anotherTask.balanceThresholdIntraNode;
        this.maxBalanceDepth = anotherTask.maxBalanceDepth;
        this.isHeavyTask = anotherTask.isHeavyTask;
        this.numSplitInternode = anotherTask.numSplitInternode;
    }


    public void resetTask(int vid, LocalSearchTask task, int startInstructionIndex) {
        for (int i = 0; i < result.length; i++) {
            result[i].clear();//clear the existing results
        }
        this.startInstructionIndex = startInstructionIndex;
        this.i = startInstructionIndex; // reset the instruction index
        this.centerVertexID = vid;
        this.centerVertexTask = task;
        this.deleted = false;
        this.isHeavyTask = false;
    }

    /**
     * Split the current task into subtasks on the current ENU instruction.
     * @param nextCandidateIndex the next candidate index to scan
     * @return the generated subtasks
     */
    public final List<LocalSearchSubTask> splitOnCurrentENUInstruction(int nextCandidateIndex,
                                                                       int maxSplit, int threshold) throws IllegalStateException {
        if (executionPlan.type[i] != 3) throw new IllegalStateException("Cannot split with non-ENU instruction, i=" + i + ", type=" + executionPlan.type[i]);
        // fx := Interset(Cy) id is OpertorID , i.e. Cy's Id
        // candidate list is Cy
        int candidateList[] = result[i].elements();
        int candidateListLen = result[i].size();
        LocalSearchSubTask baseSubTask = new LocalSearchSubTask(this);
        int numberOfNewTasks = Math.min(maxSplit, candidateListLen - nextCandidateIndex);
        numberOfNewTasks = Math.min(numberOfNewTasks, (candidateListLen - nextCandidateIndex + threshold - 1) / threshold);
        ArrayList<LocalSearchSubTask> newSubtasks = new ArrayList<>();
        int stride = numberOfNewTasks;
        int cover = 0;
        for (int startIndex = 0; startIndex < stride; startIndex++) {
            LocalSearchSubTask newTask = new LocalSearchSubTask(baseSubTask, i);
            // X saves the current value for forloop and the index of value in the candidate set
            IntArrayList X = newTask.result[i];
            X.clear();
            X.add(0); // init X[0] = 0
            X.add(1); // init X[1] = 1
            // X[0] is the value of fx, X[1] is the current index of the candidate list
            // X[2...] is the candidate list
            for (int index = nextCandidateIndex + startIndex; index < candidateListLen; index += stride) {
                X.add(candidateList[index]);
                cover++;
            }
            newTask.startInstructionIndex = i;
            newSubtasks.add(newTask);
        }
        if (cover != candidateListLen - nextCandidateIndex) throw new IllegalStateException("cover != candidate list len");
        return newSubtasks;
    }

    public final TaskExecutionReport execute(GraphWithTimestampDBClient graphStorage,
                                             MatchesCollectorInterface resultCollector,
                                             int numThread,
                                             int numRound) throws Exception {
        //System.err.println("[Enumerator] executeVertexTask...");
        long localCount = 0L;
        long dbqCount = 0L;
        long intersectionCount = 0L;
        long enuCount = 0L;
        long dbQueryingTime = 0L;
        int numSplit = numRound == 1 ? numSplitInternode : numThread;
        int balanceThreshold[] = numRound == 1 ? balanceThresholdInterNode : balanceThresholdIntraNode;
        long t0 = System.nanoTime();
        ArrayList<LocalSearchSubTask> splittedTasks = new ArrayList<>(); // new splitted tasks generated by this task
        IntArrayList filterVertices = new IntArrayList();
        // temp result set for INT instruction
        IntArrayList tmp = new IntArrayList();
        int[] reportedMathes = new int[executionPlan.patternVertexNum + 1];
        while (i >= startInstructionIndex
                && i < executionPlan.instructionsNum) { // do not backtrack out of the scope of current
            int t = executionPlan.type[i];
            int id = -1;
            IntArrayList X; // X points to the target variable of the current instruction
            switch (t) {
                case 0:
                    // INI
                    int fx = this.centerVertexID;
                    X = result[i];
                    X.clear();
                    X.add(fx);
                    i++;
                    break;
                case 1:
                    // DBQ
                    if(executionPlan.DBQType[i] == AdjType.DELTA) {
                        // Get delta forward adj from centerVertexTask
                        X = result[i];
                        X.clear();
                        X.addElements(0, centerVertexTask.getDeltaAdj());
                        //int taskA1[] = Arrays.copyOfRange(centerVertexTask.getAdj(), centerVertexTask.getStart(), centerVertexTask.getEnd() + 1);
                        //X.addElements(0, taskA1);
                        i++;
                    } else {
                        // Get old or new adj.
                        dbqCount++;
                        id = executionPlan.DBQ[i];
                        long tstart = System.nanoTime();
                        int[] Ax = graphStorage.get(result[id].getInt(0),
                                executionPlan.DBQType[i],
                                executionPlan.DBQDirection[i],
                                !deleted, timestamp);
                        long tend = System.nanoTime();
                        dbQueryingTime += tend - tstart;
                        if (Ax.length < 1) {
                            i =
                                    executionPlan.lastForLoopIndex[i] == -1 ?
                                            executionPlan.instructionsNum
                                            : executionPlan.lastForLoopIndex[i]; // backtrack
                        } else {
                            X = result[i];
                            X.clear();
                            X.addElements(0, Ax);
                            i++;
                        }
                    }
                    break;
                case 2:
                    // INT
                    intersectionCount++;
                    X = result[i];
                    X.clear();
                    int operand1 = executionPlan.INT[i][0];
                    int operand2 = executionPlan.INT[i][1];

                    if(operand2 == 0) {
                        // there are only one operand. Cx:=Intersect(Tx) or Cx:=Intersect(Ax)
                        // X.addAll(result[operand1]);
                        // filter vertices which don't satisfy injective condition or automorphism condition
                        filterVertices.clear();
                        if(executionPlan.injectiveConds[i] != null) {
                            for (int injectiveId : executionPlan.injectiveConds[i]) {
                                filterVertices.add(result[injectiveId].getInt(0));
                            }
                        }
                        int min = -1;
                        int max = -1;
                        if(executionPlan.automorphismConds[i] != null) {
                            for (int autoId : executionPlan.automorphismConds[i]) {
                                if (!CommonFunctions.isSignBit1(autoId)) {// >autoId
                                    min = Math.max(min, result[autoId].getInt(0));
                                } else { // <autoId
                                    int temp = result[autoId].getInt(0);
                                    if(max < 0 || max > temp)
                                        max = temp;
                                }
                            }
                        }
                        // the value add to X must !=v in filterVertices, >lowerBound, <upperBound
                        final int operandLen = result[operand1].size();
                        for (int j = 0; j < operandLen; j++) {
                            int value = result[operand1].getInt(j);
                            int v = CommonFunctions.setSignBitTo0(value);
                            if(filterVertices.contains(v)) continue;
                            if(v <= min) continue;
                            if(max >= 0 && v >= max) break;
                            X.add(value);
                        }
                    } else {
                        // there are two or more operands. Tx:=Intersect(X, Y, ...)
                        CommonFunctions.intersectTwoSortedArrayAdaptive(X, result[operand1], result[operand2]);
                        for(int k = 2; k < executionPlan.INT[i].length; k++) {
                            if(executionPlan.INT[i][k] != 0) {
                                tmp.clear();
                                tmp.addAll(X);
                                int operand = executionPlan.INT[i][k];
                                X.clear();
                                CommonFunctions.intersectTwoSortedArrayAdaptive(X, tmp, result[operand]);
                                if(X.size() < 1) break;
                            } else {
                                break;
                            }
                        }
                    }
                    if(X.size() < 1)
                        i = executionPlan.lastForLoopIndex[i] == -1
                                ? executionPlan.instructionsNum
                                : executionPlan.lastForLoopIndex[i]; // backtrack
                    else i++;
                    break;
                case 3:
                    // ENU
                    id = executionPlan.ENU[i]; // fx := Interset(Cy) id is OpertorID , i.e. Cy's Id
                    // save the current value for forloop and the index of value in the candidate set
                    X = result[i];
                    // result[i][0] is the value of fx, result[i][1] is the current index in the candidate set,
                    // result[i][2..] is the list of candidate vertices of this ENU instruction
                    enuCount++;
                    if(X.isEmpty()) { // generate the initial candidate set
                            X.add(0); // the current value for X[0]
                            X.add(1); // the initial index of the candidate set is 1 to make sure the nextIndex becomes 2 after the first process.
                            for (int j = 0; j < result[id].size(); j++) {
                                X.add(result[id].getInt(j));
                            }
                    }
                    int nextIndex = X.getInt(1) + 1;
                    if (nextIndex >= X.size()) { // finish traversing Cy
                        X.clear();
                        i = executionPlan.lastForLoopIndex[i] == -1
                                    ? executionPlan.instructionsNum
                                    : executionPlan.lastForLoopIndex[i]; // backtrack
                    } else { // there is remaining tasks to do
                        final int depth = executionPlan.matchedVertexCounter[i];
                        if (enableBalance && depth <= balanceThreshold.length)
                        {
                            final int numRemainedCandidates = X.size() - nextIndex;
                            final int threshold = balanceThreshold[depth-1];
                            if (numRemainedCandidates <= threshold) {
                                // do not split, execute the ENU normally
                                final int newValue = X.getInt(nextIndex);
                                if (depth == 2) {
                                    // if fx is the second vertex to match, so Cy = GETDELTAFORWARDADJ()
                                    deleted = CommonFunctions.isSignBit1(newValue);
                                }
                                X.set(0, CommonFunctions.setSignBitTo0(newValue));
                                X.set(1, nextIndex);
                                i++;
                            } else {
                                // split the task
                                List<LocalSearchSubTask> newSubTasks =
                                        splitOnCurrentENUInstruction(nextIndex, numSplit, threshold);
                                splittedTasks.addAll(newSubTasks);
                                X.clear();
                                if (numRound == 1) isHeavyTask = true;
                                i = executionPlan.lastForLoopIndex[i] == -1
                                        ? executionPlan.instructionsNum
                                        : executionPlan.lastForLoopIndex[i]; // backtrack
                            }
                        } else {
                            // no load balance
                            final int newValue = X.getInt(nextIndex);
                            if (depth == 2) {
                                // if fx is the second vertex to match, so Cy = GETDELTAFORWARDADJ()
                                deleted = CommonFunctions.isSignBit1(newValue);
                            }
                            X.set(0, CommonFunctions.setSignBitTo0(newValue));
                            X.set(1, nextIndex);
                            i++;
                        }
                    }
                    break;
                case 4:
                    // RES
                    localCount++;
                    int op = deleted ? -1 : 1;
                    for(int j = 0; j < executionPlan.patternVertexNum; j++) {
                        int targetId = executionPlan.patternVertexToENUMap[j][1];
                        reportedMathes[j] = result[targetId].getInt(0);
                    }
                    reportedMathes[executionPlan.patternVertexNum] = op;
                    resultCollector.offer(reportedMathes, op);
                    i = executionPlan.lastForLoopIndex[i] == -1
                            ? executionPlan.instructionsNum
                            : executionPlan.lastForLoopIndex[i]; // backtrack
                    break;
                case 5:
                    // InSetTest(fId,dbqId)
                    int fId = executionPlan.INS[i][0];
                    int dbqId = executionPlan.INS[i][1];
                    int fValue = result[fId].getInt(0);
                    IntArrayList dbqValue = result[dbqId];
                    if(!dbqValue.contains(fValue))
                        i = executionPlan.lastForLoopIndex[i] == -1
                                ? executionPlan.instructionsNum
                                : executionPlan.lastForLoopIndex[i]; // backtrack
                    else i++;
                    break;
                default:
                    throw new Exception("Illegal instruction type!");
            }

        }

        long t1 = System.nanoTime();
        VertexTaskStats stats = new VertexTaskStats(this.centerVertexID, this.centerVertexTask.getDeltaAdj().length,
                splittedTasks.size(),
                localCount, (t1 - t0), HostInfoUtil.getHostName(), dbqCount, intersectionCount, dbQueryingTime, enuCount);
        return new TaskExecutionReport(stats, splittedTasks);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalSearchSubTask subTask = (LocalSearchSubTask) o;
        return timestamp == subTask.timestamp &&
                startInstructionIndex == subTask.startInstructionIndex &&
                i == subTask.i &&
                centerVertexID == subTask.centerVertexID &&
                deleted == subTask.deleted &&
                enableBalance == subTask.enableBalance &&
                maxBalanceDepth == subTask.maxBalanceDepth &&
                isHeavyTask == subTask.isHeavyTask &&
                numSplitInternode == subTask.numSplitInternode &&
                Arrays.equals(result, subTask.result) &&
                executionPlan.equals(subTask.executionPlan) &&
                Objects.equals(centerVertexTask, subTask.centerVertexTask) &&
                Arrays.equals(balanceThresholdIntraNode, subTask.balanceThresholdIntraNode) &&
                Arrays.equals(balanceThresholdInterNode, subTask.balanceThresholdInterNode);
    }

    @Override
    public int hashCode() {
        int hashcode = Objects.hash(timestamp, startInstructionIndex, i, centerVertexID, deleted);
        for (int j = 0; j <= i; j++) {
            hashcode = 31 * hashcode + Arrays.hashCode(result[i].elements());
        }
        return hashcode;
    }
}
