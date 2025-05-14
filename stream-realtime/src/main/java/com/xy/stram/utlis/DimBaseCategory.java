package com.xy.stram.utlis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.xy.dwd.DimBaseCategory
 * @Author xinyi.jiao
 * @Date 2025/5/14 18:59
 * @description: base category all data
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {
    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
