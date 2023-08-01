// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.*;

import java.util.List;

public class LakeSoulRecordSet implements RecordSet {

    @Override
    public List<Type> getColumnTypes() {
        return null;
    }

    @Override
    public RecordCursor cursor() {
        return null;
    }
}



