package com.xy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.jiao.bean.TableProcessDwd
 * @Author xinyi.jiao
 * @Date 2025/5/04  14:12
 * @description: 1
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
