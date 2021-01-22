package cn.edu.nju.pasa.graph.analysis.subgraph.util;

import cn.edu.nju.pasa.graph.analysis.subgraph.util.ExecutorStatusService;
import cn.edu.nju.pasa.graph.analysis.subgraph.util.ReportStatusGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.atomic.AtomicInteger;

public class ClusterStatusClient {
    private static volatile ClusterStatusClient executorLevelClient = null;
    private final ReportStatusGrpc.ReportStatusBlockingStub stub;
    private final Thread statusReportThread;
    private final AtomicInteger numActivePartition;
    private double proportionOfBusyExecutor = 0.0;
    private int underUtilSeconds = 0;

    private ClusterStatusClient (String serverHostName) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(serverHostName, 8383).usePlaintext(true).build();
        stub = ReportStatusGrpc.newBlockingStub(channel);
        numActivePartition = new AtomicInteger(0);
        statusReportThread = new Thread(() -> {
            String hostname = HostInfoUtil.getHostName();
            while (true) {
                int isBusy = numActivePartition.intValue();
                ExecutorStatusService.Report report = ExecutorStatusService.Report.newBuilder()
                        .setHost(hostname).setIsBusy(isBusy).build();
                stub.statusReport(report);
                ExecutorStatusService.ClusterStatus status = stub
                        .getClusterStatus(ExecutorStatusService.Empty.newBuilder().build());
                proportionOfBusyExecutor = 1.0 - (double)status.getNumIdleNode() / status.getNumTotalNode();
                if (proportionOfBusyExecutor < 0.5)
                    underUtilSeconds++;
                else
                    underUtilSeconds = 0;
                System.out.println("Under util second:" + underUtilSeconds);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });
        statusReportThread.setDaemon(true);
        statusReportThread.setName("Executor Status Report Thread");
        statusReportThread.start();
    }

    public static ClusterStatusClient getExecutorLevelClient(String serverHostName) {
        if (executorLevelClient != null) return executorLevelClient;
        synchronized (ClusterStatusClient.class) {
            if (executorLevelClient == null)
                executorLevelClient = new ClusterStatusClient(serverHostName);
        }
        return executorLevelClient;
    }

    public void startActivePartition() {
        numActivePartition.incrementAndGet();
    }

    public void finishActivePartition() {
        numActivePartition.decrementAndGet();
    }

    public double getProportionOfBusyExecutor() {
        return proportionOfBusyExecutor;
    }
    public int getUnderUtilSeconds() {
        return underUtilSeconds;
    }
}
