// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class LakeSoulQueryRunner extends DistributedQueryRunner {


    public LakeSoulQueryRunner(Session defaultSession, int node) throws Exception {
        super(defaultSession, node);
    }


    public static LakeSoulQueryRunner createLakeSoulQueryRunner()
            throws Exception
    {
        LakeSoulQueryRunner queryRunner = null;
        try {
            queryRunner = new LakeSoulQueryRunner(createSession(), 1);
            Map<String, String> properties = new HashMap<>();
            properties.put("fs.s3a.access.key", "0Sba95RmPuhJZoO1olnv");
            properties.put("fs.s3a.secret.key", "N8zr0ctloiueLPcg6pbE8yhjgw3l3vIRA3BpBVon");
            properties.put("fs.s3a.bucket", "prestotest");
            properties.put("fs.s3a.endpoint", "http://localhost:9000");
            queryRunner.installPlugin(new LakeSoulPlugin());
            queryRunner.createCatalog(
                    LakeSoulConnectorFactory.CONNECTOR_NAME,
                    LakeSoulConnectorFactory.CONNECTOR_NAME,
                    properties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(LakeSoulConnectorFactory.CONNECTOR_NAME)
                .setSchema("default")
                .build();
    }

    public void shutdown()
    {
        close();
    }
}
