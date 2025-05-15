package com.xy.stram.utlis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.xy.dwd.JoinDeIn
 * @Author xinyi.jiao
 * @Date 2025/5/15 15:58
 * @description: 1
 */
public class JoinDeIn extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject detail, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        result.put("id", detail.getJSONObject("after").getInteger("id"));
        result.put("order_id", detail.getJSONObject("after").getInteger("order_id"));
        result.put("sku_id", detail.getJSONObject("after").getInteger("sku_id"));
        result.put("sku_name", detail.getJSONObject("after").getString("sku_name"));
        result.put("sku_num", detail.getJSONObject("after").getInteger("sku_num"));
        result.put("create_time", info.getJSONObject("after").getString("create_time"));
        result.put("user_id", info.getJSONObject("after").getInteger("user_id"));
        result.put("total_amount", info.getJSONObject("after").getBigDecimal("total_amount"));
        result.put("original_total_amount", info.getJSONObject("after").getBigDecimal("original_total_amount"));
        result.put("ts_ms", info.getLong("ts_ms"));
        collector.collect(result);
    }
}
