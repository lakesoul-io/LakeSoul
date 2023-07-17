// THIS FILE IS PART OF THE ZEPPELIN PROJECT

// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlSplitter {

    // it must be either 1 character or 2 character
    private Set<String> singleLineCommentPrefixList = new HashSet<>();

    public SqlSplitter() {
        this.singleLineCommentPrefixList.add("--");
    }

    /**
     * @param additionalSingleCommentPrefixList Besides the standard single line comment prefix '--',
     *                                          you can also specify other characters for sql dialect
     */
    public SqlSplitter(String... additionalSingleCommentPrefixList) {
        for (String singleLineCommentPrefix : additionalSingleCommentPrefixList) {
            if (singleLineCommentPrefix.length() > 2) {
                throw new RuntimeException("Invalid singleLineCommentPrefix: " + singleLineCommentPrefix +
                        ", it is at most 2 characters");
            }
            this.singleLineCommentPrefixList.add(singleLineCommentPrefix);
        }
    }

    /**
     * Split whole text into multiple sql statements.
     * Two Steps:
     * Step 1, split the whole text into multiple sql statements.
     * Step 2, refine the results. Replace the preceding sql statements with empty lines, so that
     * we can get the correct line number in the parsing error message.
     * <p>
     * e.g.
     * select a from table_1;
     * select a from table_2;
     * The above text will be splitted into:
     * sql_1: select a from table_1
     * sql_2: \nselect a from table_2
     *
     * @param text
     * @return
     */
    public List<String> splitSql(String text) {
        List<String> queries = new ArrayList<>();
        StringBuilder query = new StringBuilder();
        char character;

        boolean multiLineComment = false;
        boolean singleLineComment = false;
        boolean singleQuoteString = false;
        boolean doubleQuoteString = false;

        for (int index = 0; index < text.length(); index++) {
            character = text.charAt(index);

            // end of single line comment
            if (singleLineComment && (character == '\n')) {
                singleLineComment = false;
                query.append(character);
                if (index == (text.length() - 1) && !query.toString().trim().isEmpty()) {
                    // add query when it is the end of sql.
                    queries.add(query.toString());
                }
                continue;
            }

            // end of multiple line comment
            if (multiLineComment && (index - 1) >= 0 && text.charAt(index - 1) == '/'
                    && (index - 2) >= 0 && text.charAt(index - 2) == '*') {
                multiLineComment = false;
            }

            if (character == '\'' && !(singleLineComment || multiLineComment)) {
                if (singleQuoteString) {
                    singleQuoteString = false;
                } else if (!doubleQuoteString) {
                    singleQuoteString = true;
                }
            }

            if (character == '"' && !(singleLineComment || multiLineComment)) {
                if (doubleQuoteString && index > 0) {
                    doubleQuoteString = false;
                } else if (!singleQuoteString) {
                    doubleQuoteString = true;
                }
            }

            if (!singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment
                    && text.length() > (index + 1)) {
                if (isSingleLineComment(text.charAt(index), text.charAt(index + 1))) {
                    singleLineComment = true;
                } else if (text.charAt(index) == '/' && text.length() > (index + 2)
                        && text.charAt(index + 1) == '*' && text.charAt(index + 2) != '+') {
                    multiLineComment = true;
                }
            }

            if (character == ';' && !singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment) {
                // meet the end of semicolon
                if (!query.toString().trim().isEmpty()) {
                    queries.add(query.toString());
                    query = new StringBuilder();
                }
            } else if (index == (text.length() - 1)) {
                // meet the last character
                if ((!singleLineComment && !multiLineComment)) {
                    query.append(character);
                }

                if (!query.toString().trim().isEmpty()) {
                    queries.add(query.toString());
                    query = new StringBuilder();
                }
            } else if (!singleLineComment && !multiLineComment) {
                // normal case, not in single line comment and not in multiple line comment
                query.append(character);
            } else if (character == '\n') {
                query.append(character);
            }
        }

        List<String> refinedQueries = new ArrayList<>();
        for (int i = 0; i < queries.size(); ++i) {
            String emptyLine = "";
            if (i > 0) {
                emptyLine = createEmptyLine(refinedQueries.get(i - 1));
            }
            if (isSingleLineComment(queries.get(i)) || isMultipleLineComment(queries.get(i))) {
                // refine the last refinedQuery
                if (refinedQueries.size() > 0) {
                    String lastRefinedQuery = refinedQueries.get(refinedQueries.size() - 1);
                    refinedQueries.set(refinedQueries.size() - 1,
                            lastRefinedQuery + createEmptyLine(queries.get(i)));
                }
            } else {
                String refinedQuery = emptyLine + queries.get(i);
                refinedQueries.add(refinedQuery);
            }
        }

        return refinedQueries;
    }

    private boolean isSingleLineComment(String text) {
        return text.trim().startsWith("--");
    }

    private boolean isMultipleLineComment(String text) {
        return text.trim().startsWith("/*") && text.trim().endsWith("*/");
    }

    private String createEmptyLine(String text) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < text.length(); ++i) {
            if (text.charAt(i) == '\n') {
                builder.append('\n');
            }
        }
        return builder.toString();
    }

    private boolean isSingleLineComment(char curChar, char nextChar) {
        for (String singleCommentPrefix : singleLineCommentPrefixList) {
            if (singleCommentPrefix.length() == 1) {
                if (curChar == singleCommentPrefix.charAt(0)) {
                    return true;
                }
            }
            if (singleCommentPrefix.length() == 2) {
                if (curChar == singleCommentPrefix.charAt(0) &&
                        nextChar == singleCommentPrefix.charAt(1)) {
                    return true;
                }
            }
        }
        return false;
    }
}