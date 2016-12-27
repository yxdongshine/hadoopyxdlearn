package com.ibeifeng.transformer.hive.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.ibeifeng.transformer.common.GlobalConstants;
import com.ibeifeng.transformer.dimension.key.base.KpiDimension;
import com.ibeifeng.transformer.service.converter.IDimensionConverter;
import com.ibeifeng.transformer.service.converter.impl.DimensionConverterImpl;

/**
 * 根据给定的维度的kpi名称返回对应的id的值
 * 
 * @author ibf
 *
 */
public class KpiDimensionConverterUDF extends UDF {
	/**
	 * 应用于维度转换的实际类
	 */
	private static IDimensionConverter converter = new DimensionConverterImpl();

	/**
	 * 根据给定的kpi的名称返回对应的id的值
	 * 
	 * @param name
	 * @param version
	 * @return
	 */
	public int evaluate(String name) {
		if (StringUtils.isBlank(name)) {
			// 给定的名称为空
			name = GlobalConstants.DEFAULT_VALUE;
		}

		// 构建kpi维度对象
		KpiDimension kpi = new KpiDimension(name);
		// 获取id的值
		try {
			return converter.getDimensionIdByValue(kpi);
		} catch (IOException e) {
			throw new IllegalArgumentException("根据给定的kpi名称获取id的值异常" + name, e);
		}
	}
}
