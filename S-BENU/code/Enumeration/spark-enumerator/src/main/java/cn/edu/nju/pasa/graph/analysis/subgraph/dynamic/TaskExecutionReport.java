package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;

import java.io.Serializable;
import java.util.List;

final public class TaskExecutionReport implements Serializable {
    private VertexTaskStats status;

    public VertexTaskStats getStatus() {
        return this.status;
    }

    public List<LocalSearchSubTask> getGeneratedSubTasks() {
        return generatedSubTasks;
    }

    private List<LocalSearchSubTask> generatedSubTasks;

    public TaskExecutionReport(VertexTaskStats status, List<LocalSearchSubTask> splittedTasks) {
        this.status = status;
        this.generatedSubTasks = splittedTasks;
    }
}
