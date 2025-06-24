// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.lakesoul.functions.CosineDistanceFunction;
import com.facebook.presto.lakesoul.functions.HammingDistanceMatchFunction;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class LakeSoulPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LakeSoulConnectorFactory());
    }

    @Override public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(CosineDistanceFunction.class)
                .add(HammingDistanceMatchFunction.class)
                .build();
    }
}
