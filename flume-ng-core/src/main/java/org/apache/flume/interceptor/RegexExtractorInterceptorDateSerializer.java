/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.interceptor;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Serializer that converts the passed in value into milliseconds using the
 * specified formatting pattern
 */
public class RegexExtractorInterceptorDateSerializer implements RegexExtractorInterceptorSerializer {
	private static final Logger logger = LoggerFactory.getLogger(RegexExtractorInterceptorDateSerializer.class);
	private SimpleDateFormat df;
	private String datetype = "";

	@Override
	public void configure(Context context) {
		String pattern = context.getString("pattern");
		datetype = context.getString("datetype", "default");
		Preconditions.checkArgument(!StringUtils.isEmpty(pattern), "Must configure with a valid pattern");
		df = new SimpleDateFormat(pattern);
		df.setLenient(false);
	}

	@Override
	public String serialize(String value) {
		if (datetype.equals("timestamp")) {
			if (value.indexOf(".") > 0) {
				value = value.substring(0, value.indexOf("."));
			}
			Long timestamp = 0L;
			try {
				if (value.length() == 10) {
					timestamp = Long.parseLong(value) * 1000;
				} else if (value.length() == 13) {
					timestamp = Long.parseLong(value);
				} else {
					logger.error("wrong timestamp;please check");
					return null;
				}
			} catch (java.lang.NumberFormatException e) {
				timestamp = System.currentTimeMillis();
			}

			return df.format(new Date(timestamp));
		}
		if (datetype.equals("MMMd")) {
			SimpleDateFormat formatParse = new SimpleDateFormat("MMM dd", Locale.ENGLISH);
			SimpleDateFormat format = new SimpleDateFormat("MMdd");
			SimpleDateFormat formatYear = new SimpleDateFormat("yyyy");
			Date parse = null;
			try {
				parse = formatParse.parse(value);
			} catch (ParseException e) {
				e.printStackTrace();
				parse = new Date();
			}
			return formatYear.format(new Date()) + format.format(parse);
		}

		if (datetype.equals("default")) {
			SimpleDateFormat dfdefault = new SimpleDateFormat("yyyy-MM-dd");
			dfdefault.setLenient(false);
			try {
				return df.format(dfdefault.parse(value));
			} catch (ParseException f) {
				logger.error("Date parse failed. Exception follows." + f + " error date:" + value);
				return null;
			}
		} else {
			SimpleDateFormat dfdefault = new SimpleDateFormat(datetype, Locale.ENGLISH);
			dfdefault.setLenient(false);
			try {
				return df.format(dfdefault.parse(value));
			} catch (ParseException f) {
				logger.error("Date parse failed. Exception follows." + f + " error date:" + value);
				return null;
			}

		}
	}

	@Override
	public void configure(ComponentConfiguration conf) {
		// NO-OP...
	}
}
