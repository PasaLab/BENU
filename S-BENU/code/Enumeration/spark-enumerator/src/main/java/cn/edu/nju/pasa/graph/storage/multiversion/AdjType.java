package cn.edu.nju.pasa.graph.storage.multiversion;

public enum AdjType {
    DELTA_FORWARD,
    OLD_FORWARD,
    OLD_REVERSE,
    NEW_FORWARD,
    NEW_REVERSE,
    EITHER,
    DELTA,
    UNALTERED,
    FORWARD,
    REVERSE
}
