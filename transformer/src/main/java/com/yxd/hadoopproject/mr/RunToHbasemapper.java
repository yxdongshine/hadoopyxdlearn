package com.yxd.hadoopproject.mr;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.ibeifeng.transformer.common.EventLogConstants;
import com.ibeifeng.transformer.common.GlobalConstants;
import com.ibeifeng.transformer.common.EventLogConstants.EventEnum;
import com.ibeifeng.transformer.mr.etl.AnalysisDataMapper;
import com.ibeifeng.transformer.util.LoggerUtil;
import com.ibeifeng.transformer.util.TimeUtil;

/**
 * 
 * @author yxd
 *   因为这是数据过滤，输出到和base中，所以这里不需要reduce
 */
public class RunToHbasemapper extends Mapper<Object, Text, NullWritable, Put> {
	
	//  定义日志 辅助查找问题
	private static final Logger logger = Logger.getLogger(RunToHbasemapper.class);
	private long currentMillis = -1L;
	private CRC32 crc32 = new CRC32();
	private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;

	/**
	 *    setup函数在一个应用中只运行一次，在mapreduce执行之前
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		//获取conf中自定义参数值
		this.currentMillis = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));

		
	}
	
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String logText = value.toString();
		Map<String, String> clientInfo = LoggerUtil.handleLogText(logText);
		if (clientInfo == null || clientInfo.isEmpty()) {
			// 结束当前记录的处理，表示日志解析失败
			logger.debug("日志解析失败，当前日志数据为:" + logText);
			return;
		}
		
		// 2.2 判断解析之后的数据是否异常，比如是否缺少数据
				if (!this.filterEventData(clientInfo, logText)) {
					// 表示有必须存在的字段属性在clientInfo中没有找到
					logger.debug("当前数据缺少关键性字段，日志为:" + logText);
					return;
				}
				
				
				
				
				
				
				//合法后构建hbase put
				

				// 1. 构建rowkey
				String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
				long serverTime = Long.valueOf(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME));
				//循环教研，产生唯一编号
				byte[] rowkey = this.generateRowKey(uuid, serverTime, clientInfo);

				// 2. 构建put
				Put put = new Put(rowkey);
				// 2.1 将clientInfo中的属性值添加到Put中
				for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
					String key1 = entry.getKey();
					String value1 = entry.getValue();
                    //按参数列表产生列簇
					if (StringUtils.isNotBlank(key1) && StringUtils.isNotBlank(value1)) {
						put.add(family, Bytes.toBytes(key1), Bytes.toBytes(value1));
					}
				}

				// 3. 返回put
				context.write(NullWritable.get(), put);
			
				
	}
	
	/**
	 * 根据给定的参数产生rowkey， rowkey: 随机 + 尽可能的短小<br/>
	 * 随机值 + 时间戳(当前过期的时间毫米数)
	 * 
	 * @param uuid
	 * @param serverTime
	 * @param clientInfo
	 * @return
	 */
	private byte[] generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo) {
		this.crc32.reset(); // 重置
		if (StringUtils.isNotBlank(uuid)) {
			this.crc32.update(Bytes.toBytes(uuid));
		}
		this.crc32.update(Bytes.toBytes(clientInfo.hashCode()));
		byte[] buf1 = Bytes.toBytes(this.crc32.getValue()); // 占用8个字节

		byte[] buf2 = Bytes.toBytes((int) (serverTime - this.currentMillis)); // 占用4个字节

		// 合并两个buffer
		byte[] buffer = new byte[buf1.length + buf2.length];
		System.arraycopy(buf1, 0, buffer, 0, buf1.length);
		System.arraycopy(buf2, 0, buffer, buf1.length, buf2.length);

		return buffer;
	}
	private boolean filterEventData(Map<String, String> clientInfo, String logText) {
		// 1. 获取数据类型 ==> 事件类型
		String eventAliasName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
		EventEnum event = EventEnum.valueOfAlias(eventAliasName);

		// 2. 过滤无效的数据类型
		if (event == null) {
			// 没法处理该事件类型的数据
			logger.warn("没法处理当前数据对应的事件类型，事件值是:" + eventAliasName + ", 日志为:" + logText);
			return false;
		}

		// 3. 构建filter数据过滤
		// 3.1 构建共同的字段属性的过滤，比如platform、servertime...
		String platform = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM);
		String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
		boolean result = StringUtils.isNotBlank(platform) && StringUtils.isNotBlank(serverTime);

		// 3.2 针对不同的平台、不同的事件进行数据过滤判断
		if (result) {
			switch (platform) {
			case EventLogConstants.PlatformNameConstants.PC_WEBSITE_SDK:
				// WEB 平台
				// 3.2.1 考虑web平台上公用的字段属性，比如会话id、访客id
				result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID))
						&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID));

				// 3.2.2 考虑不同事件的字段属性
				switch (event) {
				case LAUNCH:
					// nothings
					break;
				case PAGEVIEW:
					// 要求当前页面的url必须存在
					result = result
							&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL));
					break;
				case EVENT:
					// 要求ca和ac必须存在
					// TODO: 自己完善
					break;
				case CHARGEREQUEST:
					// 要求订单id、金额、支付方式、货币类型必须存在
					// TODO: 自己完善
					break;
				default:
					// web平台上不处理了该事件的数据
					result = false;
					break;
				}
				break;
			case EventLogConstants.PlatformNameConstants.JAVA_SERVER_SDK:
				// JAVA 后台
				// 要求会员id必须存在
				// TODO: 自己完善
				break;
			default:
				// 没法处理该平台的数据
				result = false;
				break;
			}
		}

		// 4. 返回结果
		return result;
	}

}
