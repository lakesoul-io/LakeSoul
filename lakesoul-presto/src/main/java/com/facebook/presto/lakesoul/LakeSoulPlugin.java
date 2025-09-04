// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.lakesoul.functions.CosineDistanceFunction;
import com.facebook.presto.lakesoul.functions.HammingDistanceMatchFunction;
import com.facebook.presto.lakesoul.security.LakeSoulSystemAccessControlFactory;
import com.facebook.presto.lakesoul.security.LakesoulPrestoAuthenticatorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class LakeSoulPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new LakeSoulConnectorFactory());
    }

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(CosineDistanceFunction.class)
                .add(HammingDistanceMatchFunction.class)
                .build();
    }

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        return ImmutableList.of(new LakeSoulSystemAccessControlFactory());
    }

    @Override
    public Iterable<PrestoAuthenticatorFactory> getPrestoAuthenticatorFactories() {
        return ImmutableList.of(new LakesoulPrestoAuthenticatorFactory());
    }
}
