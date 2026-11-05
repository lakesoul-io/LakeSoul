// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.security;

import com.facebook.presto.spi.security.PrestoAuthenticator;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;

import java.util.Map;

public class LakesoulPrestoAuthenticatorFactory implements PrestoAuthenticatorFactory {
    private static final String name = "lakesoul";

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PrestoAuthenticator create(Map<String, String> config) {
        return new LakesoulPrestoAuthenticator();
    }
}
