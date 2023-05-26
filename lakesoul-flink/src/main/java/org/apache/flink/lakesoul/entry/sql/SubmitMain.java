/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

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
