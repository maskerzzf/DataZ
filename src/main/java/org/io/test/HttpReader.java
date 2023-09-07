package org.io.test;

import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONTokener;
import cn.hutool.json.JSONUtil;

import java.util.HashMap;

public class HttpReader {
    public static void main(String[] args) {
        String s = HttpUtil.get("http://fine-doc.oss-cn-shanghai.aliyuncs.com/book.json");
        JSONObject jsonObject = JSONUtil.parseObj(s);
        JSON json = JSONUtil.parse(s);
    }
}
