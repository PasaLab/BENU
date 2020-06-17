package cn.edu.nju.pasa.graph.plangen.incremental;

public class InstructionNotation {
    public static final boolean DEBUG = false;
    public static final boolean INJECTIVE_FILTER = true;
    public static final boolean DEGREE_FILTER = false;
    public static final boolean COMPRESS = false;
    public static final String DATAGRAPH_SEPARATOR = " ";
    public static final String ENUTARGET = "f";
    public static final String DBQTARGET = "A";
    public static final String DBQ_DELTA_FORWARD_TARGET = "ADO";
    public static final String DBQ_EITHER_FORWARD_TARGET = "AEO";
    public static final String DBQ_EITHER_REVERSE_TARGET = "AEI";
    public static final String DBQ_UNALTERED_FORWARD_TARGET = "AUO";
    public static final String DBQ_UNALTERED_REVERSE_TARGET = "AUI";
    public static final String INTTARGET = "T";
    public static final String INTCANDIDATE = "C";
    public static final String DATAVERTEXSET = "V";

    public static final String INIT = "Init";
    public static final String DBQ = "GetAdj";
    public static final String DBQ_TYPE_EITHER = "either";
    public static final String DBQ_TYPE_DELTA = "delta";
    public static final String DBQ_TYPE_UNALTERED = "unaltered";
    public static final String DBQ_FORWARD = "out";
    public static final String DBQ_REVERSE = "in";
    public static final String SET_INTERSECTION = "Intersect";
    public static final String ENUMERATION = "Foreach";
    public static final String RESULT_REPORTING = "ReportMatch";
}
