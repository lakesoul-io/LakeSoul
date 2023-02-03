/*
 *
 *
 *   Copyright [2022] [DMetaSoul Team]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.spark.entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

public class PipelineParser {
    public PipeLineContainer parserYaml(String yamlPath) {
        File yamlFile = new File(yamlPath);
        if (!yamlFile.exists()) {
            System.out.println("yamlFile not exist in path : " + yamlPath);
        }
        ObjectMapper obm = new ObjectMapper(new YAMLFactory());
        PipeLineContainer pc = null;
        try {
            pc = obm.readValue(yamlFile, PipeLineContainer.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pc;

    }
}
