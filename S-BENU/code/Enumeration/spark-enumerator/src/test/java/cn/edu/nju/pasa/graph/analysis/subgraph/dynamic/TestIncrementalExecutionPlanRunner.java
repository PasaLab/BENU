package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.DynamicSubgraphEnumerationGeneric2;
import cn.edu.nju.pasa.graph.storage.multiversion.AdjType;
import cn.edu.nju.pasa.graph.storage.multiversion.GraphWithTimestampDBClient;
import com.sun.org.apache.xpath.internal.operations.Bool;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import scala.Tuple2;
import scala.Tuple3;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.*;

import java.io.File;
import java.util.stream.Collectors;

public class TestIncrementalExecutionPlanRunner {

    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(ExecutionPlanTestCase.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure);
        }
        System.out.println(result.wasSuccessful());
    }
}

class ExecutionPlanTestCase extends TestCase {
    String planDir = "src/main/java/resources/execplan/dynamic/diamond";

    @Before
    public void setUp() {

    }
    @ParameterizedTest
    @ValueSource(strings = {"src/main/java/resources/execplan/dynamic/clique4/1.txt",
    "src/main/java/resources/execplan/dynamic/clique4/2.txt",
    "src/main/java/resources/execplan/dynamic/clique4/3.txt",
    "src/main/java/resources/execplan/dynamic/clique4/4.txt"})
    public void testExecutionPlanGeneration(String planPath) throws Exception {
        File planFile = new File(planPath);
        assertTrue(planFile.exists());
        String execPlan[] = DynamicSubgraphEnumerationGeneric2.loadExecutionPlan(planFile);
        EnumeratorWithNoDB enumerator = new EnumeratorWithNoDB(execPlan.length);
        enumerator.construct(execPlan);
        assertEquals(enumerator.instructionsNum, execPlan.length);
        IncrementalExecutionPlan iPlan = new IncrementalExecutionPlan(execPlan);
        assertEquals(iPlan.instructionsNum, execPlan.length);
        comparePlan(enumerator, iPlan);
    }

    void compareNestedArray(int arr1[][], int arr2[][]) {
        assertTrue(arr1.length == arr2.length);
        for (int i = 0; i < arr1.length; i++) {
            assertTrue(Arrays.equals(arr1[i], arr2[i]));
        }
    }

    private void comparePlan(EnumeratorWithNoDB enumerator, IncrementalExecutionPlan iPlan) {
        assertEquals(enumerator.patternVertexNum, iPlan.patternVertexNum);
        assertEquals(enumerator.type.length, iPlan.type.length);
        assertTrue(Arrays.equals(enumerator.type, iPlan.type));
        assertTrue(java.util.Arrays.equals(enumerator.DBQ, iPlan.DBQ));
        assertTrue(java.util.Arrays.equals(enumerator.DBQType, iPlan.DBQType));
        assertTrue(java.util.Arrays.equals(enumerator.DBQDirection, iPlan.DBQDirection));
        assertTrue(Arrays.equals(enumerator.ENU, iPlan.ENU));
        assertTrue(Arrays.equals(enumerator.lastForLoopIndex, iPlan.lastForLoopIndex));
        assertTrue(Arrays.equals(enumerator.matchedVertexCounter, iPlan.matchedVertexCounter));
        assertTrue(Arrays.equals(enumerator.matchedVertexCounter, iPlan.matchedVertexCounter));
        compareNestedArray(enumerator.INT, iPlan.INT);
        compareNestedArray(enumerator.patternVertexToENUMap, iPlan.patternVertexToENUMap);
        compareNestedArray(enumerator.injectiveConds, iPlan.injectiveConds);
        compareNestedArray(enumerator.automorphismConds, iPlan.automorphismConds);
        compareNestedArray(enumerator.INS, iPlan.INS);
    }

}

