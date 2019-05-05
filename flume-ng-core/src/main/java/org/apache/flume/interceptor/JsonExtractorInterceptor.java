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

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

/**
 * Interceptor that extracts matches using a specified regular expression and
 * appends the matches to the event headers using the specified serializers
 * </p>
 * Note that all regular expression matching occurs through Java's built in
 * java.util.regex package
 * </p>
 * . Properties:
 * <p>
 * regex: The regex to use
 * <p>
 * serializers: Specifies the group the serializer will be applied to, and the
 * name of the header that will be added. If no serializer is specified for a
 * group the default {@link RegexExtractorInterceptorPassThroughSerializer} will
 * be used
 * <p>
 * Sample config:
 * <p>
 * agent.sources.r1.channels = c1
 * <p>
 * agent.sources.r1.type = SEQ
 * <p>
 * agent.sources.r1.interceptors = i1
 * <p>
 * agent.sources.r1.interceptors.i1.type = REGEX_EXTRACTOR
 * <p>
 * agent.sources.r1.interceptors.i1.regex = (WARNING)|(ERROR)|(FATAL)
 * <p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2
 * agent.sources.r1.interceptors.i1.serializers.s1.type =
 * com.blah.SomeSerializer
 * agent.sources.r1.interceptors.i1.serializers.s1.name = warning
 * agent.sources.r1.interceptors.i1.serializers.s2.type =
 * org.apache.flume.interceptor.RegexExtractorInterceptorTimestampSerializer
 * agent.sources.r1.interceptors.i1.serializers.s2.name = error
 * agent.sources.r1.interceptors.i1.serializers.s2.dateFormat = yyyy-MM-dd
 * </code>
 * </p>
 *
 * <pre>
 * Example 1:
 * </p>
 * EventBody: 1:2:3.4foobar5</p> Configuration:
 * agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 * </p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2 s3
 * agent.sources.r1.interceptors.i1.serializers.s1.name = one
 * agent.sources.r1.interceptors.i1.serializers.s2.name = two
 * agent.sources.r1.interceptors.i1.serializers.s3.name = three
 * </p>
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2, three=3
 *
 * Example 2:
 *
 * EventBody: 1:2:3.4foobar5
 *
 * Configuration: agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 *
 * agent.sources.r1.interceptors.i1.serializers = s1 s2
 * agent.sources.r1.interceptors.i1.serializers.s1.name = one
 * agent.sources.r1.interceptors.i1.serializers.s2.name = two
 *
 *
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2
 * </pre>
 */
public class JsonExtractorInterceptor implements Interceptor {

  static final String REGEX = "regex";
  static final String SERIALIZERS = "serializers";

  private static final Logger logger = LoggerFactory
          .getLogger(JsonExtractorInterceptor.class);

  private final Pattern regex;
  private final List<NameAndSerializer> serializers;
  private final Context jsoncontext;
  private final Context multicontext;
  private final Context urlParamContext;
  private final Context serializerscontext;

  private JsonExtractorInterceptor(Pattern regex,
                                   List<NameAndSerializer> serializers, Context jsoncontext, Context multicontext, Context urlParamContext,
                                   Context serializerscontext) {
    this.regex = regex;
    this.jsoncontext = jsoncontext;
    this.serializers = serializers;
    this.multicontext = multicontext;
    this.urlParamContext = urlParamContext;
    this.serializerscontext = serializerscontext;
  }

  @Override
  public void initialize() {
    // NO-OP...
  }

  @Override
  public void close() {
    // NO-OP...
  }

  private static Map<String, String> convert(String str) {
    Map<String, String> map = new HashMap<String, String>();
    try{
      JSONObject jsondata = JSONObject.fromObject(str);
      convertJson(jsondata, map,"");
    }catch(Exception e){
      return Maps.newHashMap();
    }
    return map;
  }

  private static void convertJson(JSONObject json, Map<String, String> map,String prefix) {
    for (Iterator<?> iter = json.keys(); iter.hasNext();) {
      String key = (String) iter.next();
      String value = json.getString(key);
      if(key.equals("data")){
        map.put(prefix+"_"+key, value.toString());
      }else{
        try {
          JSONObject.fromObject(value);
          convertJson(JSONObject.fromObject(value), map,prefix+"_"+key);
        } catch (JSONException e) {
          map.put(prefix+"_"+key, value.toString());
        }
      }

    }
  }

  @Override
  public Event intercept(Event event) {
    Matcher matcher = regex.matcher(
            new String(event.getBody(), Charsets.UTF_8));
    Map<String, String> headers = event.getHeaders();
    if (matcher.find()) {
      for (int group = 0, count = matcher.groupCount(); group < count; group++) {
        int groupIndex = group + 1;
        if (groupIndex > serializers.size()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Skipping group {} to {} due to missing serializer",
                    group, count);
          }
          break;
        }
        NameAndSerializer serializer = serializers.get(group);

        if (logger.isDebugEnabled()) {
          logger.debug("Serializing {} using {}", serializer.headerName,
                  serializer.serializer);
        }

        if (serializer.headerName.equals("json")) {
          String jsonstr = matcher.group(groupIndex);
          if (jsoncontext.containsKey("convert")) {

            switch (jsoncontext.getString("convert")) {
              case "unbase64": {
                try {
                  jsonstr = new String(Base64.decode(jsonstr), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
                  jsonstr = null;
                }

              }
              default:
                ;
            }

          }
          String jsonrawListStr = jsoncontext.getString("raw");
          String jsonnewListStr = jsoncontext.getString("new");
          String[] rawNames = jsonrawListStr.split("\\s+");
          String[] newNames = jsonnewListStr.split("\\s+");
          if (newNames != null && rawNames.length != newNames.length) {
            logger.error("config error,json raw names not equal new names");
            logger.debug(jsonstr);
            return event;
          }
          Map<String, String> map;
          try {
            map = convert(jsonstr);
          } catch (net.sf.json.JSONException e) {
            headers.put(null, null);
            logger.error("Could not convert json.", e + " json=" + matcher.group(groupIndex));
            logger.debug(jsonstr);
            return event;
          }

          for (int i = 0; i < rawNames.length; i++) {
            String key = rawNames[i];
            String name = (newNames == null) ? key : newNames[i];
            String value = (map.containsKey(rawNames[i])) ? map.get(key) : "";
            Context jsonfieldcontext = new Context(
                    jsoncontext.getSubProperties(name + "."));
            String type = jsonfieldcontext.getString("type", "DEFAULT");
            if ("DEFAULT".equals(type)) {
              headers.put(name, value);
            } else {
              RegexExtractorInterceptorSerializer jsonserializer;
              try {
                jsonserializer = (RegexExtractorInterceptorSerializer) Class
                        .forName(type).newInstance();
                jsonserializer.configure(jsonfieldcontext);
                headers.put(name, jsonserializer.serialize(value));
              } catch (Exception e) {
                logger.error("Could not instantiate jsonevent serializer.", e);
                Throwables.propagate(e);
                headers.put(null, null);
                return event;
              }
            }
          }
        } else if (serializer.headerName.equals("urlParam")) {
          String urlParamRawStr = urlParamContext.getString("raw");
          String urlParamNewStr = urlParamContext.getString("new");
          String[] rawNames = urlParamRawStr.split("\\s+");
          String[] newNames = null;
          if (StringUtils.isEmpty(urlParamNewStr)) {
            newNames = rawNames;
          } else {
            newNames = urlParamNewStr.split("\\s+");
          }
          if (newNames != null && rawNames.length != newNames.length) {
            logger.error("config error,urlParam raw names not equal new names");
            continue;
          }
          String paramStr = matcher.group(groupIndex);
          Map<String, String> map;
          try {
            map = Splitter.on("&").omitEmptyStrings().withKeyValueSeparator("=").split(paramStr.trim());
          } catch (Exception e) {
            e.printStackTrace();
            logger.error("paramStr split 2 map error,paramStr:" + paramStr);
            continue;
          }
          for (int i = 0; i < rawNames.length; i++) {
            String rawName = rawNames[i];
            String newName = newNames[i];
            extractMap(rawName, newName, map, urlParamContext, headers);
          }

        } else if (serializer.headerName.equals("multi")) {
          String jsonnewListStr = multicontext.getString("new");
          String[] newNames = jsonnewListStr.split("\\s+");
          for (int i = 0; i < newNames.length; i++) {
            String name = newNames[i];
            Context jsonfieldcontext = new Context(
                    multicontext.getSubProperties(name + "."));
            String type = jsonfieldcontext.getString("type", "DEFAULT");
            if ("DEFAULT".equals(type)) {
              headers.put(name, serializer.serializer.serialize(matcher.group(groupIndex)));
            } else {
              RegexExtractorInterceptorSerializer multiserializer;
              try {
                multiserializer = (RegexExtractorInterceptorSerializer) Class
                        .forName(type).newInstance();
                multiserializer.configure(jsonfieldcontext);
                headers.put(name, multiserializer
                        .serialize(serializer.serializer.serialize(matcher.group(groupIndex))));
              } catch (Exception e) {
                logger.error("Could not instantiate jsonevent serializer.", e);
                Throwables.propagate(e);
                headers.put(null, null);
                return event;
              }
            }
          }
        } else {
          headers.put(serializer.headerName,
                  serializer.serializer.serialize(matcher.group(groupIndex)));
        }

      }

    }
    logger.debug(event.getHeaders().toString());
    return event;
  }

  private void extractMap(String rawName, String newName, Map<String, String> map, Context context,
                          Map<String, String> headers) {
    String value = map.containsKey(rawName) ? map.get(rawName) : "";
    Context subContext = new Context(
            context.getSubProperties(newName + "."));
    String type = subContext.getString("type", "DEFAULT");
    if ("DEFAULT".equals(type)) {
      headers.put(newName, value);
    } else {
      RegexExtractorInterceptorSerializer serializer;
      try {
        serializer = (RegexExtractorInterceptorSerializer) Class
                .forName(type).newInstance();
        serializer.configure(context);
        headers.put(newName, serializer.serialize(value));
      } catch (Exception e) {
        logger.error("Could not instantiate context serializer.", e);
        Throwables.propagate(e);
        return;
      }
    }
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
    for (Event event : events) {
      Event interceptedEvent = intercept(event);
      if (interceptedEvent != null) {
        intercepted.add(interceptedEvent);
      }
    }
    return intercepted;
  }

  public static class Builder implements Interceptor.Builder {

    private Pattern regex;
    private List<NameAndSerializer> serializerList;
    private final RegexExtractorInterceptorSerializer defaultSerializer = new RegexExtractorInterceptorPassThroughSerializer();
    private Context serializerContextsForJson;
    private Context serializerContextsForMulti;
    private Context serializerContextsForUrlParam;
    private Context serializerContexts;

    @Override
    public void configure(Context context) {
      String regexString = context.getString(REGEX);
      Preconditions.checkArgument(!StringUtils.isEmpty(regexString),
              "Must supply a valid regex string");
      regex = Pattern.compile(regexString);
      regex.pattern();
      regex.matcher("").groupCount();
      configureSerializers(context);
    }

    private void configureSerializers(Context context) {

      String serializerListStr = context.getString(SERIALIZERS);
      Preconditions.checkArgument(!StringUtils.isEmpty(serializerListStr),
              "Must supply at least one name and serializer");

      String[] serializerNames = serializerListStr.split("\\s+");
      serializerContexts = new Context(context.getSubProperties(SERIALIZERS + "."));
      serializerContextsForJson = new Context(
              serializerContexts.getSubProperties("json."));
      ;
      serializerList = Lists.newArrayListWithCapacity(serializerNames.length);
      serializerContextsForMulti = new Context(
              serializerContexts.getSubProperties("multi."));

      serializerContextsForUrlParam = new Context(serializerContexts.getSubProperties("urlParam."));

      for (String serializerName : serializerNames) {
        Context serializerContext = new Context(
                serializerContexts.getSubProperties(serializerName + "."));
        String type = serializerContext.getString("type", "DEFAULT");
        String name = serializerContext.getString("name");
        Preconditions.checkArgument(!StringUtils.isEmpty(name),
                "Supplied name cannot be empty.");

        if ("DEFAULT".equals(type)) {
          serializerList.add(new NameAndSerializer(name, defaultSerializer));
        } else {
          serializerList.add(new NameAndSerializer(name, getCustomSerializer(
                  type, serializerContext)));
        }

      }
    }

    private RegexExtractorInterceptorSerializer getCustomSerializer(
            String clazzName, Context context) {
      try {
        RegexExtractorInterceptorSerializer serializer = (RegexExtractorInterceptorSerializer) Class
                .forName(clazzName).newInstance();
        serializer.configure(context);
        return serializer;
      } catch (Exception e) {
        logger.error("Could not instantiate event serializer.", e);
        Throwables.propagate(e);
      }
      return defaultSerializer;
    }

    @Override
    public Interceptor build() {
      Preconditions.checkArgument(regex != null,
              "Regex pattern was misconfigured");
      Preconditions.checkArgument(serializerList.size() > 0,
              "Must supply a valid group match id list");
      return new JsonExtractorInterceptor(regex, serializerList, serializerContextsForJson,
              serializerContextsForMulti, serializerContextsForUrlParam, serializerContexts);
    }
  }

  static class NameAndSerializer {
    private final String headerName;
    private final RegexExtractorInterceptorSerializer serializer;

    public NameAndSerializer(String headerName,
                             RegexExtractorInterceptorSerializer serializer) {
      this.headerName = headerName;
      this.serializer = serializer;
    }
  }
}