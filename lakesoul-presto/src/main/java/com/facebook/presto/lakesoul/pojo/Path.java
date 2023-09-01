// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Path extends org.apache.hadoop.fs.Path {
    private String filename;

    @JsonCreator
    public Path(@JsonProperty("filename") String filename) {
        super(filename);
        this.filename = filename;
    }


    @JsonProperty
    public String getFilename(){
        return this.filename;
    }

}