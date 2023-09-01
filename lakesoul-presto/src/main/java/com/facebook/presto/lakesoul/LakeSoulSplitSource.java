// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;

public class LakeSoulSplitSource extends FixedSplitSource implements ConnectorSplitSource {
    public LakeSoulSplitSource(Iterable<? extends ConnectorSplit> splits) {
        super(splits);
    }
}
