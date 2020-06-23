package cn.edu.nju.pasa.graph.plangen;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ExecutionInstruction {
    private InstructionType type;
    private String targetVariable;
    private String singleOperator = "";
    private List<String> multiOperators = new ArrayList<String>();
    private List<Integer> automorphismBreakingConds = new ArrayList<Integer>(); // the condition >2,<1 is stored as [2,-1]
    private List<Integer> injectiveConds = new ArrayList<>(); // the condition ≠1,≠2 is stored as [1,2]
    private int degreeCondition = -1;
    private int sizeCondition = -1;
    private List<String> dependOn = new ArrayList<String>();

    public ExecutionInstruction() {
    }

    public ExecutionInstruction(InstructionType type, String targetVariable, String singleOperator) {
        this.type = type;
        this.targetVariable = targetVariable;
        this.singleOperator = singleOperator;
    }

    public ExecutionInstruction(InstructionType type, String targetVariable, List<String> multiOperators) {
        this.type = type;
        this.targetVariable = targetVariable;
        this.multiOperators = new ArrayList<>(multiOperators);
    }

    public ExecutionInstruction(InstructionType type,
                                String targetVariable,
                                String singleOperator,
                                List<Integer> automorphismBreakingConds,
                                List<Integer> injectiveConds
                                /*int degreeCondition*/) {
        this.type = type;
        this.targetVariable = targetVariable;
        this.singleOperator = singleOperator;
        this.automorphismBreakingConds = new ArrayList<>(automorphismBreakingConds);
        this.injectiveConds = new ArrayList<>(injectiveConds);
        //this.degreeCondition = degreeCondition;
    }

    public ExecutionInstruction(ExecutionInstruction instruction) {
        this.type = instruction.getType();
        this.targetVariable = instruction.getTargetVariable();
        this.singleOperator = instruction.getSingleOperator();
        this.multiOperators = new ArrayList<>(instruction.getMultiOperators());
        this.automorphismBreakingConds = new ArrayList<>(instruction.getAutomorphismBreakingConds());
        this.injectiveConds = new ArrayList<>(instruction.getInjectiveConds());
        this.degreeCondition = instruction.getDegreeCondition();
        this.sizeCondition = instruction.getSizeCondition();
        this.dependOn = new ArrayList<>(instruction.getDependOn());
    }

    public void setType(InstructionType type) {
        this.type = type;
    }

    public void setTargetVariable(String targetVariable) {
        this.targetVariable = targetVariable;
    }

    public void setSingleOperator(String singleOperator) {
        this.singleOperator = singleOperator;
        this.multiOperators.clear();
    }

    public void setDegreeCondition(int degreeCondition) {
        this.degreeCondition = degreeCondition;
    }

    public void setMultiOperators(List<String> multiOperators) {
        this.multiOperators = new ArrayList<>(multiOperators);
        this.singleOperator = "";
    }

    public void setAutomorphismBreakingConds(List<Integer> condition) {
        this.automorphismBreakingConds = new ArrayList<>(condition);
    }

    public void setInjectiveConds(List<Integer> condition) {
        this.injectiveConds = new ArrayList<>(condition);
    }

    public void addAutomorphismConds(int c) {
        this.automorphismBreakingConds.add(c);
    }

    public void addInjectiveConds(int c) {
        this.injectiveConds.add(c);
    }

    public void setSizeCondition(int c) {
        this.sizeCondition = c;
    }

    public void setDependOn(List<String> dependOn) {
        this.dependOn = new ArrayList<>(dependOn);
    }

    public InstructionType getType() {
        return type;
    }

    public String getTargetVariable() {
        return targetVariable;
    }

    public String getSingleOperator() {
        return singleOperator;
    }

    public List<String> getMultiOperators() {
        return new ArrayList<>(multiOperators);
    }

    public List<Integer> getAutomorphismBreakingConds() {
        return new ArrayList<>(automorphismBreakingConds);
    }

    public List<Integer> getInjectiveConds() {
        return new ArrayList<>(injectiveConds);
    }

    public int getSizeCondition() {return sizeCondition;}

    public int getDegreeCondition() {return degreeCondition;}

    public List<String> getDependOn() {
        return new ArrayList<>(dependOn);
    }

    public boolean isSingleOperator() {
        return !singleOperator.equals("");
    }

    public boolean hasFilterCondition() {
        return (degreeCondition!=-1 || automorphismBreakingConds.size() > 0);
    }

    @Override
    public String toString() {
        StringBuilder instruction = new StringBuilder();
        String operation = "";
        if(type == InstructionType.INI) {
            instruction.append(targetVariable + ":=" + "Init(start)");
        } else {
            switch (type) {
                case DBQ:
                    operation = "GetAdj"; break;
                case INT:
                    operation = "Intersect"; break;
                case ENU:
                    operation = "Foreach"; break;
                case TRC:
                    operation = "TCache"; break;
                case RES:
                    operation = "ReportMatch";
            }

            instruction.append(targetVariable + ":=" + operation + "(");
            if(!singleOperator.equals("")) {
                instruction.append(singleOperator + ")");
            } else {
               for(String operator: multiOperators) {
                   instruction.append(operator + ",");
               }
               instruction.deleteCharAt(instruction.length() - 1);
               instruction.append(")");
            }

            boolean condition = false;

            if(!automorphismBreakingConds.isEmpty()) {
                condition = true;
                instruction.append(" | ");
                for(Integer c: automorphismBreakingConds) {
                    String op = "";
                    if(c<0) {
                        op = "<";
                    } else if(c>0) {
                        op = ">";
                    }
                    int vid = Math.abs(c);
                    instruction.append(op + ConstantCharacter.ENUTARGET + vid + ",");
                }
            }

            if(!injectiveConds.isEmpty()) {
                if(!condition) {
                    condition = true;
                    instruction.append(" | ");
                }
                for(Integer c: injectiveConds) {
                    instruction.append("≠" + ConstantCharacter.ENUTARGET + c + ",");
                }
            }

            if(condition) instruction.deleteCharAt(instruction.length() - 1);

            if(degreeCondition != -1) {
                if(!condition) {
                    condition = true;
                    instruction.append(" | ");
                } else {
                    instruction.append(" & ");
                }
                instruction.append("DEG(" + "u" + degreeCondition + ")");
            }

            if(sizeCondition > 1) {
                if(!condition) {
                    instruction.append(" | ");
                } else {
                    instruction.append(" & ");
                }
                instruction.append("SIZE>=" + sizeCondition);
            }
        }

        //System.out.println(instruction);

        return instruction.toString();

    }

    public enum InstructionType {
        //  INI < INT < TRC < DBQ < ENU < RES
        INI,
        INT,
        TRC,
        DBQ,
        ENU,
        RES
    }
}
