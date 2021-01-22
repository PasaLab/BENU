package cn.edu.nju.pasa.graph.analysis.subgraph.util;

import cn.edu.nju.pasa.graph.analysis.subgraph.util.ExecutorStatusService;
import cn.edu.nju.pasa.graph.analysis.subgraph.util.ReportStatusGrpc;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

public class ClusterStatusServer extends ReportStatusGrpc.ReportStatusImplBase {

    private final int totalAvailableCores;
    private final IntSupplier numActiveTaskSupplier;
    private final ConcurrentHashMap<String, Boolean> nodeStatus = new ConcurrentHashMap<>();
    private final ExecutorStatusService.Empty empty = ExecutorStatusService.Empty.newBuilder().build();

    public ClusterStatusServer(int availableCores, IntSupplier numActiveTaskSupplier) {
        this.totalAvailableCores = availableCores;
        this.numActiveTaskSupplier = numActiveTaskSupplier;
    }

    @Override
    public void statusReport(ExecutorStatusService.Report request, StreamObserver<ExecutorStatusService.Empty> responseObserver) {
        String host = request.getHost();
        boolean isBusy = (request.getIsBusy() > 0) ? true : false;
        nodeStatus.put(host, isBusy);
        responseObserver.onNext(empty);
        responseObserver.onCompleted();
    }

    @Override
    public void getClusterStatus(ExecutorStatusService.Empty request, StreamObserver<ExecutorStatusService.ClusterStatus> responseObserver) {
        /*
        int totalNode = nodeStatus.size();
        int busyNode = nodeStatus.reduceValuesToInt(16, isBusy -> isBusy ? 1 : 0, 0, (a,b) -> {return a + b;});
        */
        int totalCore = totalAvailableCores;
        int numActiveTask = numActiveTaskSupplier.getAsInt();
        ExecutorStatusService.ClusterStatus status = ExecutorStatusService.ClusterStatus.newBuilder()
                .setNumIdleNode(totalCore - numActiveTask).setNumTotalNode(totalCore).build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }
}
