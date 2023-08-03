// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.alibaba.fastjson.JSON;

public class JsonUtil {

    public static <T> T parse(String str, Class<T> cls){
        return JSON.parseObject(str, cls);
    }

    public static <T> String format(T object){
        return JSON.toJSONString(object);
    }
}
