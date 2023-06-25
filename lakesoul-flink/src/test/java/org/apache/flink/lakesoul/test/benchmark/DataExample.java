/*
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
 */

package org.apache.flink.lakesoul.test.benchmark;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;

public class DataExample {
    public int col_1;
    // this version 14.5 with bug dealing with char type
    //    public char col_2;
    public String col_3;
    public String col_4;
    public boolean col_5;
    public BigDecimal col_6;
    public Byte col_7;
    public short col_8;
    public int col_9;
    public long col_10;
    public float col_11;
    public double col_12;
    public Date col_13;
    // spark 3.3.1 version not support timestamp without timezone: timestamp_ntz
    // public Timestamp col_14;
    public Instant col_15;

    public DataExample() {
    }

    public DataExample(int col_1, String col_3, String col_4, boolean col_5, BigDecimal col_6, Byte col_7, short col_8, int col_9, long col_10, float col_11, double col_12, Date col_13, Instant col_15) {
        this.col_1 = col_1;
//        this.col_2 = col_2;
        this.col_3 = col_3;
        this.col_4 = col_4;
        this.col_5 = col_5;
        this.col_6 = col_6;
        this.col_7 = col_7;
        this.col_8 = col_8;
        this.col_9 = col_9;
        this.col_10 = col_10;
        this.col_11 = col_11;
        this.col_12 = col_12;
        this.col_13 = col_13;
//        this.col_14 = col_14;
        this.col_15 = col_15;
    }

    @Override
    public String toString() {
        return "DataExample{" +
                "col_1=" + col_1 +
//                ", col_2='" + col_2 + '\'' +
                ", col_3='" + col_3 + '\'' +
                ", col_4='" + col_4 + '\'' +
                ", col_5=" + col_5 +
                ", col_6=" + col_6 +
                ", col_7=" + col_7 +
                ", col_8=" + col_8 +
                ", col_9=" + col_9 +
                ", col_10=" + col_10 +
                ", col_11=" + col_11 +
                ", col_12=" + col_12 +
                ", col_13=" + col_13 +
//                ", col_14=" + col_14 +
                ", col_15=" + col_15 +
                '}';
    }
}
