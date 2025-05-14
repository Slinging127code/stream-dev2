package com.xy.func.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.stream.domain.DimCategoryCompare
 * @Author xinyi.jiao
 * @Date 2025/5/14 10:57
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
