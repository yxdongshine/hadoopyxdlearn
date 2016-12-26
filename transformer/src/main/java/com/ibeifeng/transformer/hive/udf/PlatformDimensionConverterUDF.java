package com.ibeifeng.transformer.hive.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.ibeifeng.transformer.common.GlobalConstants;
import com.ibeifeng.transformer.dimension.key.base.PlatformDimension;
import com.ibeifeng.transformer.service.converter.IDimensionConverter;
import com.ibeifeng.transformer.service.converter.impl.DimensionConverterImpl;

/**
 * 根据给定的维度的名称和version返回对应的id的值
 * 
 * @author ibf
 *
 */
public class PlatformDimensionConverterUDF extends UDF {
	/**
	 * 应用于维度转换的实际类
	 */
	private static IDimensionConverter converter = new DimensionConverterImpl();

	/**
	 * 根据给定的platform的名称和版本返回对应的id的值
	 * 
	 * @param name
	 * @param version
	 * @return
	 */
	public int evaluate(String name, String version) {
		if (StringUtils.isBlank(name)) {
			// 给定的名称为空
			name = GlobalConstants.DEFAULT_VALUE;
			version = GlobalConstants.DEFAULT_VALUE;
		}

		if (StringUtils.isBlank(version)) {
			// version为空，但是name不为空
			version = GlobalConstants.DEFAULT_VALUE;
		}

		// 构建platform维度对象
		PlatformDimension platform = new PlatformDimension(name, version);
		// 获取id的值
		try {
			return converter.getDimensionIdByValue(platform);
		} catch (IOException e) {
			throw new IllegalArgumentException("根据给定的平台名称和版本获取id的值异常" + name + "," + version, e);
		}
	}
}
