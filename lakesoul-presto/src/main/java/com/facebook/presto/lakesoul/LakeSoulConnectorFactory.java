// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

public class LakeSoulConnectorFactory implements ConnectorFactory {
    public static final String CONNECTOR_NAME = "lakesoul";
    private final LakeSoulMetadata metadata = new LakeSoulMetadata();
    private final LakeSoulSplitManager manager = new LakeSoulSplitManager();
    private final LakeSoulRecordSetProvider provider = new LakeSoulRecordSetProvider();
    private final ConnectorHandleResolver handleResolver = new LakeSoulHandleResolver();

    @Override
    public String getName() {
        return CONNECTOR_NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return handleResolver;
    }

    @Override
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context) {
        LakeSoulConfig.initInstance(config);
        return new LakeSoulConnector(metadata, manager, provider);
    }

}
