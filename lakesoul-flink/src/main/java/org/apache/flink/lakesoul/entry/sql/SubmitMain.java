// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql;

import org.apache.flink.lakesoul.entry.sql.common.SubmitOption;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

public class SubmitMain {
    private static final Logger LOG = LoggerFactory.getLogger(SubmitMain.class);

    public static void main(String[] args) throws IOException, URISyntaxException {
        for (String arg : args) {
            LOG.info("arg: {}", arg);
        }
        SubmitOption submitOption = optionBuild(args);
        Submitter submitter = SubmitterFactory.createSubmitter(submitOption);
        submitter.submit();
    }

    private static SubmitOption optionBuild(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        return new SubmitOption(params);
    }
}
