/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class simply writes the body of the event to the output stream
 * and appends a newline after each event.
 */
public class DelimitedTextEventSerializer implements EventSerializer {
	public static final String ALIAS = "DELIMITED";

	public static final String DEFAULT_CHARSET = "UTF-8";

	public static final String CHARSET = "charset";
	public static final String DELIMITER = "delimiter";
	public static final String FIELD_NAMES = "fieldnames";

	private String delimiter;
	private String charset;

	private String[] inputColNames;

	private byte[] data;
	private final static Logger logger =
			LoggerFactory.getLogger(DelimitedTextEventSerializer.class);

	// for legacy reasons, by default, append a newline to each event written out
	private final String APPEND_NEWLINE = "appendNewline";
	private final boolean APPEND_NEWLINE_DFLT = true;

	private final OutputStream out;
	private final boolean appendNewline;

	private DelimitedTextEventSerializer(OutputStream out, Context ctx) {
		this.appendNewline = ctx.getBoolean(APPEND_NEWLINE, APPEND_NEWLINE_DFLT);
		this.out = out;
		String DEFAULT_DELIMITER="";
		try {
			DEFAULT_DELIMITER=new String(new byte[] {1} ,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			DEFAULT_DELIMITER=",";
		}
		delimiter = parseDelimiterSpec(ctx.getString(DELIMITER, DEFAULT_DELIMITER));
		charset = ctx.getString(CHARSET, DEFAULT_CHARSET);
		String fieldNames = Preconditions.checkNotNull(ctx.getString(FIELD_NAMES), "Field names cannot be empty, " +
				"please specify in configuration file");
		inputColNames = fieldNames.split(",", -1);
	}

	@Override
	public boolean supportsReopen() {
		return true;
	}

	@Override
	public void afterCreate() {
		// noop
	}

	@Override
	public void afterReopen() {
		// noop
	}

	@Override
	public void beforeClose() {
		// noop
	}

	// if delimiter is a double quoted like "\t", drop quotes
	private static String parseDelimiterSpec(String delimiter) {
		if (delimiter == null) {
			return null;
		}
		if (delimiter.charAt(0) == '"' && delimiter.charAt(delimiter.length() - 1) == '"') {
			return delimiter.substring(1, delimiter.length() - 1);
		}
		return delimiter;
	}


	@Override
	public void write(Event e){
		String outStr="";
		Map<String,String> map=e.getHeaders();
//	  if(map.size()<inputColNames.length)return;
		int num=0;
		for(int i=0;i<inputColNames.length;i++){
			String key=inputColNames[i];
			String value=(map.containsKey(key))?map.get(key):null;
			if(num==0){
				outStr=(value==null?"":value);
			}else{
				outStr=outStr+delimiter+(value==null?"":value);
			}
			num++;
		}
		try {

			if(outStr!=null){
				out.write(outStr.getBytes());
			}
			if (appendNewline) {
				out.write('\n');
			}

		} catch (IOException e1) {
			logger.error("write failed "+e);
		}

	}

	@Override
	public void flush() throws IOException {
		// noop
	}

	public static class Builder implements EventSerializer.Builder {

		@Override
		public EventSerializer build(Context context, OutputStream out) {
			DelimitedTextEventSerializer s = new DelimitedTextEventSerializer(out, context);
			return s;
		}

	}

}
