package com.xy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.jiao.bean.TableProcessDim
 * @Author xinyi.jiao
 * @Date 2025/5/04  14:35
 * @description: 实体类
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {
    //来源表名
    String sourceTable;
//目标表名
    String sinkTable;
//输出字段
    String sinkColumns;
    // 数据到 hbase 的列族
    String sinkFamily;
    // sink到hbase的时候的主键字段
    String sinkRowKey;
    //配置表操作类型
    String op;
}
