/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

public class LakeSoulConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return null;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        return null;
    }
}
