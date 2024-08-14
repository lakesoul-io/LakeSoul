// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql;

import org.apache.flink.lakesoul.entry.sql.common.SubmitOption;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

public abstract class Submitter {
    protected SubmitOption submitOption;

    public Submitter(SubmitOption submitOption) {
        this.submitOption = submitOption;
    }

    public abstract void submit() throws Exception;

}
