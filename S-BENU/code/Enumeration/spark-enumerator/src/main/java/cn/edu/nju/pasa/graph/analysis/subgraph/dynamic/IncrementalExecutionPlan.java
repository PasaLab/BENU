package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.storage.multiversion.AdjType;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * The incremental execution plan.
 */
public class IncrementalExecutionPlan implements Serializable {
    final public int instructionsNum;
    final public int patternVertexNum;
    final public int[] type; // types of execution instructions: 0(INI), 1(DBQ), 2(INT), 3(ENU), 4(RES), 5(INS)
    final public int[] DBQ; // operatorID
    final public AdjType[] DBQType; // 0(either), 1(delta), 2(unaltered)
    final public AdjType[] DBQDirection; // 0(forward), 1(reverse)
    final public int[][] INT; // operator1ID, operator2ID, ...
    final public int[] ENU; // OpertorID
    final public int[] lastForLoopIndex;
    final public int[] matchedVertexCounter;
    final public int[][] patternVertexToENUMap; // {{pattern vertex id1, ENU id}, {pattern vertex id2, ENU id}, ...}
    final public int[][] injectiveConds;
    final public int[][] automorphismConds;
    final public int[][] INS; // operator1ID(fx), operator2ID(A?Oy)

    /**
     * Create the incremental execution plan object from the text description
     * @param execPlan
     */
    public IncrementalExecutionPlan(String[] execPlan) throws IOException {
        int N = execPlan.length; // number of instructions
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
                            throw new IOException("Illegal instruction, unrecognized filter condition: " + execPlan[i]);
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
                    } else throw new IOException("Illegal instruction, unrecognized adj type: " + execPlan[i]);
                    DBQType[i] = dbqtype;

                    // store dbq direction: forward(0), reverse(1)
                    String directionOp = operands[2];
                    AdjType direction;
                    if(directionOp.equalsIgnoreCase(InstructionNotation.DBQ_FORWARD)) {
                        direction = AdjType.FORWARD;
                    } else if(directionOp.equalsIgnoreCase(InstructionNotation.DBQ_REVERSE)) {
                        direction = AdjType.REVERSE;
                    }  else throw new IOException("Illegal instruction, nrecognized adj direction: " + execPlan[i]);
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
                    throw new IOException("Illegal instruction: " + execPlan[i]);
            }
        }

        patternVertexNum = matchedVertexNum;
        Arrays.sort(patternVertexToENUMap, 0, patternVertexNum, (a, b) -> a[0] - b[0]);
    }

}
