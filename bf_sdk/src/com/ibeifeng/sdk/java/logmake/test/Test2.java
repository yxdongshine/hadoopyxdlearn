package com.ibeifeng.sdk.java.logmake.test;

import com.ibeifeng.sdk.java.logmake.AnalyticsEngineSDK;

public class Test2 {
	public static void main(String[] args) {
		String memberId = "1234234";
		
		// 在业务系统中，需要的地方调用方法即可
		AnalyticsEngineSDK.onChargeSuccess("oid123456", memberId);
		AnalyticsEngineSDK.onChargeRefund("order1512", memberId);
	}
}
