package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.DynamicSubgraphEnumerationGeneric2;
import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Properties;

public class TestDynamicSubgraphEnumerationGeneric2 {

    @Test
    public void testAdaptiveBalanceThresholdGeneration() {
        Properties prop = new Properties();
        DynamicSubgraphEnumerationGeneric2.setBalanceThreshold(prop, 5, 4, 10, 2, 8);
        Assert.assertEquals(prop.getProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTERNODE), "40,25,250");
        Assert.assertEquals(prop.getProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTRANODE), "8,5,50");

    }
}
