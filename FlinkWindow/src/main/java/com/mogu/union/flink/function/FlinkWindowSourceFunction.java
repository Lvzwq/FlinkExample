package com.mogu.union.flink.function;

import com.mogu.union.flink.common.TupleValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.charset.Charset;

/**
 * @ClassName FlinkWindowSourceFunction
 * @Description
 * @Author qiangge
 * @Date 2019-09-19 11:53
 * @Version 1.0
 **/
@Slf4j
public class FlinkWindowSourceFunction implements SourceFunction<TupleValue> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<TupleValue> sourceContext) throws Exception {
		while (running) {
			long rand = (long) (1000L * Math.random());
			long eventTime = System.currentTimeMillis() - rand;
			TupleValue value = new TupleValue();
			sourceContext.collectWithTimestamp(value, eventTime);
			Thread.sleep(rand);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}


	public static String rowKeySalter(String rowKey) {
		String padRowKey = StringUtils.leftPad(rowKey, 19, "0");
		String salt = Hashing.murmur3_32().hashString(padRowKey, Charset.()).toString();
		if (salt.length() > 8) {
			salt = salt.substring(salt.length() - 8);
		}
		return salt + padRowKey;
	}

	public static void main(String[] args) {
		System.out.println(rowKeySalter("682021669"));
	}
}
