// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import java.io.Serializable;

public class DataBaseProperty implements Serializable {
    private String driver;
    private String url;
    private String username;
    private String password;
    private int maxCommitAttempt;

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxCommitAttempt() {
        return maxCommitAttempt;
    }

    public void setMaxCommitAttempt(int maxCommitAttempt) {
        this.maxCommitAttempt = maxCommitAttempt;
    }

    @Override
    public String toString() {
        return "lakesoul.pg.driver=" +
                driver +
                "\nlakesoul.pg.url=" +
                url +
                "\nlakesoul.pg.username" +
                username +
                "\nlakesoul.pg.password" +
                password +
                "\n";
    }
}
