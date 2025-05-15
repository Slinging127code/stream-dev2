package com.xy.stram.utlis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.xy.dwd.MapOrderDelitDim
 * @Author xinyi.jiao
 * @Date 2025/5/15 15:49
 * @description: 1
 */
public class MapOrderDelitDim extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject, JSONObject order, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        Integer height = jsonObject.getInteger("height");
        String unit_height = jsonObject.getString("unit_height");
        Integer weight = jsonObject.getInteger("weight");
        String unit_weight = jsonObject.getString("unit_weight");
        Integer uid = jsonObject.getInteger("uid");
        result.put("height", height);
        result.put("unit_height", unit_height);
        result.put("weight", weight);
        result.put("unit_weight", unit_weight);
        result.put("uid", uid);
        Integer birth_decade1 = jsonObject.getInteger("birth_decade");
        Integer age = jsonObject.getInteger("age");
        Integer user_level = jsonObject.getInteger("user_level");
        Long ts_ms = jsonObject.getLong("ts_ms");
        String birthday = jsonObject.getString("birthday");
        String gender = jsonObject.getString("gender");
        String name = jsonObject.getString("name");
        String zodiacSign = jsonObject.getString("zodiac_sign");
        String createTime = jsonObject.getString("create_time");
        Integer id = jsonObject.getInteger("id");
        String birthDecade = jsonObject.getString("decade");
        String login_name = jsonObject.getString("login_name");
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
        JSONObject result1 = new JSONObject();
        result1.put("id", order.getInteger("id"));
        result1.put("order_id", order.getInteger("order_id"));
        result1.put("sku_id", order.getInteger("sku_id"));
        result1.put("sku_name", order.getString("sku_name"));
        result1.put("sku_num", order.getInteger("sku_num"));
        result1.put("create_time", order.getString("create_time"));
        result1.put("user_id", order.getInteger("user_id"));
        result1.put("total_amount", order.getBigDecimal("total_amount"));
        result1.put("original_total_amount", order.getBigDecimal("original_total_amount"));
        result1.put("ts_ms", order.getLong("ts_ms"));
        result1.put("category2_id", order.getString("category2_id"));
        result1.put("category3_id", order.getString("category3_id"));
        result1.put("category1_id", order.getString("category1_id"));
        result1.put("category1_name", order.getString("category1_name"));
        result1.put("tm_name", order.getString("tm_name"));
        result1.put("category_name", order.getString("category_name"));
        result1.put("os", order.getString("os"));
        JSONObject result2 = new JSONObject();
        result2.put("Age",result1);
        result2.put("user",result);
        collector.collect(result2);
    }
}
