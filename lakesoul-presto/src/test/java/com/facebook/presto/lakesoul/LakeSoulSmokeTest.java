// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.lakesoul.LakeSoulQueryRunner.createLakeSoulQueryRunner;


@Test(singleThreaded = true)
public class LakeSoulSmokeTest extends AbstractTestIntegrationSmokeTest {

    protected LakeSoulQueryRunner lakeSoulQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createLakeSoulQueryRunner();
    }

    @BeforeClass
    public void setUp()
    {
        lakeSoulQueryRunner = (LakeSoulQueryRunner) getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (lakeSoulQueryRunner != null) {
            lakeSoulQueryRunner.shutdown();
        }
    }

    /** for easier testing */
    protected List<MaterializedRow> sql(String query){
        MaterializedResult result = lakeSoulQueryRunner.execute(query);
        return result.getMaterializedRows();
    }

}
