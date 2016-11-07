/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ajansing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Tags({ "json", "flatten", "ajansing" })
@CapabilityDescription("This processor flattens Jsons in the content of the "
		+ "flow file and saves it back to the flow file.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class JsonFlattener extends AbstractProcessor {

	public static final PropertyDescriptor SEP = new PropertyDescriptor.Builder().name("Flattening delimeter")
			.description("This is the delimeter to be used to signify where the Json was flattened. i.e. "
					+ "first:{second... turns into first.second when the delimeter is \".\".")
			.defaultValue(".").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship ORIGINAL = new Relationship.Builder().name("Original").description("Original Json")
			.build();

	public static final Relationship FLATTENED = new Relationship.Builder().name("Flattened")
			.description("Flattened Json").build();

	public static final Relationship ERROR = new Relationship.Builder().name("Error").description("Parsing Error")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SEP);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(ORIGINAL);
		relationships.add(FLATTENED);
		relationships.add(ERROR);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		String delim = context.getProperty(SEP).getValue();

		final ComponentLog logger = getLogger();
		final Gson gson = new Gson();
		try{
			JsonObject originalJson = getJson(flowFile, session, gson);
			JsonObject flattenedJson = flattenJson(originalJson, delim);
			flowFile = writeFlowFile(flowFile, session, originalJson);
			session.transfer(flowFile, ORIGINAL);
			FlowFile flat = writeFlowFile(session.create(), session, flattenedJson);
			session.transfer(flat, FLATTENED);
		} catch (Exception e){
			logger.error("Error parsing json.", e);
			session.transfer(flowFile, ERROR);
		}
	}

	private FlowFile writeFlowFile(FlowFile flowFile, ProcessSession session, JsonObject originalJson) {
		return session.write(flowFile, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				Gson gson = new Gson();
				out.write(gson.toJson(originalJson).getBytes());
			}
		});
	}

	private JsonObject flattenJson(JsonObject originalJson, String delim) {
		Map<String, Object> target = new HashMap<>();
		Iterator<Entry<String, JsonElement>> it1 = originalJson.entrySet().iterator();
		target = flattenElements(target, it1, delim);
		Iterator<Entry<String, Object>> it2 = target.entrySet().iterator();
		JsonObject json = new JsonObject();
		Gson gson = new Gson();
		while (it2.hasNext()) {
			Entry<String, Object> token = it2.next();
			json.add(token.getKey(), gson.toJsonTree(token.getValue()));
		}
		return json;
	}

	private Map<String, Object> flattenElements(Map<String, Object> target, Iterator<Entry<String, JsonElement>> it, String delim) {
		while (it.hasNext()) {
			Entry<String, JsonElement> token = it.next();
			if (token.getValue().isJsonObject()) {
				Map<String, Object> subjson = new HashMap<>();
				subjson = flattenElements(subjson, token.getValue().getAsJsonObject().entrySet().iterator(), delim);
				target = mergeMaps(target, subjson, token.getKey(), delim);
			} else {
				target.put(token.getKey(), token.getValue());
			}
		}
		return target;
	}

	private Map<String, Object> mergeMaps(Map<String, Object> target, Map<String, Object> source, String prepender, String delim) {
		Iterator<Entry<String, Object>> it = source.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> next = it.next();
			target.put(prepender + delim + next.getKey(), next.getValue());
		}
		return target;
	}

	private JsonObject getJson(FlowFile flowFile, ProcessSession session, Gson gson) {
		final StringBuilder builder = new StringBuilder();
		JsonParser jp = new JsonParser();
		session.read(flowFile, new InputStreamCallback() {

			@SuppressWarnings("deprecation")
			@Override
			public void process(InputStream in) throws IOException {
				builder.append(IOUtils.toString(in));
			}
		});
		return jp.parse(builder.toString().replaceAll("\n", "").replaceAll("\t", "")).getAsJsonObject();
	}
}
