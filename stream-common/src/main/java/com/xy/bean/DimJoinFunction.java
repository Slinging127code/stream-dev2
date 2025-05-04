package com.xy.bean;
import com.alibaba.fastjson.JSONObject;
/**
 * @Package com.jiao.bean.DimJoinFunction
 * @Author xinyi.jiao
 * @Date 2025/5/04   14:57
 * @description: 接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
