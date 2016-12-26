package com.ibeifeng.transformer.hive.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.ibeifeng.transformer.common.DateEnum;
import com.ibeifeng.transformer.dimension.key.base.DateDimension;
import com.ibeifeng.transformer.service.converter.IDimensionConverter;
import com.ibeifeng.transformer.service.converter.impl.DimensionConverterImpl;
import com.ibeifeng.transformer.util.TimeUtil;

/**
 * 根据给定的时间类型获取时间维度对应的id
 * 
 * @author ibf
 *
 */
public class DateDimensionConverterUDF extends UDF {
	/**
	 * 应用于维度转换的实际类
	 */
	private static IDimensionConverter converter = new DimensionConverterImpl();

	/**
	 * 根据给定的date的yyyy-MM-dd格式的时间字符串以及需要的date维度的类型type获取对应的维度id
	 * 
	 * @param date
	 * @param type
	 * @return
	 */
	public int evaluate(String date, String type) {
		if (StringUtils.isBlank(date) || StringUtils.isBlank(type)) {
			// 这的两个参数不能为空
			throw new IllegalArgumentException("参数不能为空");
		}
		
		// 构建date dimension维度对象
		DateDimension dimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), DateEnum.valueOfName(type));

		// 获取id的值
		try {
			return converter.getDimensionIdByValue(dimension);
		} catch (IOException e) {
			throw new IllegalArgumentException("获取id异常", e);
		}
	}
}
