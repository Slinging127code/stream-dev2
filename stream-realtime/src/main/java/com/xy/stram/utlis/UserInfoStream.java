package com.xy.stram.utlis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.xy.dwd.domain.UserInfoStream
 * @Author xinyi.jiao
 * @Date 2025/5/15 16:00
 * @description: 1
 */
public class UserInfoStream extends RichMapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        JSONObject after = jsonObject.getJSONObject("after");
        Integer birth_decade1 = after.getInteger("birth_decade");
        Integer age = after.getInteger("age");
        Integer user_level = after.getInteger("user_level");
        Long ts_ms = after.getLong("ts_ms");
        String birthday = after.getString("birthday");
        String gender = after.getString("gender");
        String name = after.getString("name");
        String zodiacSign = after.getString("zodiac_sign");
        String createTime = after.getString("create_time");
        Integer id = after.getInteger("id");
        String birthDecade = after.getString("decade");
        String login_name = after.getString("login_name");
        result.put("birthday", birthday);
        result.put("decade", birthDecade);
        result.put("name", name);
        result.put("birth_decade",birth_decade1);
        result.put("ts_ms", ts_ms);
        result.put("zodiac_sign", zodiacSign);
        result.put("id", id);
        result.put("user_level",user_level);
        result.put("gender", gender);
        result.put("age",age);
        result.put("create_time", createTime);
        result.put("login_name", login_name);
        return result;
    }
}
