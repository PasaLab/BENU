package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

public class InstructionNotation {
    public static final String INIT = "Init";
    public static final String INIT_START = "start";
    public static final String INIT_DST = "dst";
    public static final String DBQ = "GetAdj";
    public static final String DBQ_TYPE_EITHER = "either";
    public static final String DBQ_TYPE_DELTA = "delta";
    public static final String DBQ_TYPE_UNALTERED = "unaltered";
    public static final String DBQ_FORWARD = "out";
    public static final String DBQ_REVERSE = "in";
    public static final String DBQ_DELTA_FORWARD = "GetDeltaForwardAdj";
    public static final String DBQ_OLD_FORWARD = "GetOldForwardAdj";
    public static final String DBQ_OLD_REVERSE = "GetOldReverseAdj";
    public static final String DBQ_NEW_FORWARD = "GetNewForwardAdj";
    public static final String DBQ_NEW_REVERSE = "GetNewReverseAdj";
    public static final String SET_INTERSECTION = "Intersect";
    public static final String ENUMERATION = "Foreach";
    public static final String RESULT_REPORTING = "ReportMatch";
    public static final String INS = "InSetTest";
}
