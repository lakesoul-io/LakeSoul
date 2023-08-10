// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;

public class Path {
    private String filename;

    @JsonCreator
    public Path(@JsonProperty("filename") String filename) {
        this.filename = filename;
    }

//    public Path( Path path){
//        this.filename = path.toString();
//    }

    @JsonProperty
    public String getFilename(){
        return this.filename;
    }

    public String getName(){
        String[] frags = this.filename.split(File.separator);
        return frags[frags.length - 1];
    }
}
