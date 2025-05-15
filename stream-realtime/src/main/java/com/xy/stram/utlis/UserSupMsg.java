package com.xy.stram.utlis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.xy.dwd.domain.UserSupMsg
 * @Author xinyi.jiao
 * @Date 2025/5/15 16:02
 * @description: 1
 */
public class UserSupMsg extends RichMapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        JSONObject after = jsonObject.getJSONObject("after");
        Integer height = after.getInteger("height");
        String unit_height = after.getString("unit_height");
        Integer weight = after.getInteger("weight");
        String unit_weight = after.getString("unit_weight");
        Integer uid = after.getInteger("uid");
        result.put("height", height);
        result.put("unit_height", unit_height);
        result.put("weight", weight);
        result.put("unit_weight", unit_weight);
        result.put("uid", uid);
        return result;
    }
}
