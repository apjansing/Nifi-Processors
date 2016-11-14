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
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

@Tags({ "json", "jsonarray", "flatten", "ajansing" })
@CapabilityDescription("This processor flattens Jsons in the content of the "
		+ "flow file and saves it back to the flow file.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class JsonFlattener extends AbstractProcessor {
	final Logger log = LoggerFactory.getLogger(JsonFlattener.class);
	final NifiTools nt = new NifiTools();
	final Gson gson = new Gson();

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

		try {
			JsonElement originalJson = nt.readAsJsonElement(flowFile, session);
			if (originalJson.isJsonArray()) {
				JsonArray ja = flattenJsonArray(originalJson.getAsJsonArray(), delim);
				done(originalJson, ja, flowFile, session);
			} else if (originalJson.isJsonObject()) {
				JsonObject flattenedJson = flattenJson(originalJson.getAsJsonObject(), delim);
				done(originalJson, flattenedJson, flowFile, session);
			}
		} catch (Exception e) {
			log.error("Error parsing json.", e);
			session.transfer(flowFile, ERROR);
		}
	}

	private void done(JsonElement originalJson, JsonElement jsonElement, FlowFile flowFile, ProcessSession session) {
		flowFile = writeFlowFile(flowFile, session, originalJson);
		session.transfer(flowFile, ORIGINAL);
		FlowFile flat = writeFlowFile(session.create(), session, jsonElement);
		session.transfer(flat, FLATTENED);
	}

	private JsonArray flattenJsonArray(JsonArray asJsonArray, String delim) {
		for (int i = 0; i < asJsonArray.size(); i++) {
			JsonObject json = flattenJson(asJsonArray.get(i).getAsJsonObject(), delim);
			asJsonArray.set(i, json);
		}
		return asJsonArray;
	}

	private JsonObject flattenJson(JsonObject originalJson, String delim) {
		Map<String, Object> target = new HashMap<>();
		target = flattenElements(target, originalJson.entrySet().iterator(), delim);
		Iterator<Entry<String, Object>> it2 = target.entrySet().iterator();
		JsonObject json = new JsonObject();
		Gson gson = new Gson();
		while (it2.hasNext()) {
			Entry<String, Object> token = it2.next();
			json.add(token.getKey(), gson.toJsonTree(token.getValue()));
		}
		return json;
	}

	private FlowFile writeFlowFile(FlowFile flowFile, ProcessSession session, JsonElement originalJson) {
		return session.write(flowFile, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				out.write(gson.toJson(originalJson).getBytes());
			}
		});
	}

	private Map<String, Object> flattenElements(Map<String, Object> target, Iterator<Entry<String, JsonElement>> it,
			String delim) {
		while (it.hasNext()) {
			Entry<String, JsonElement> token = it.next();
			log.info(gson.toJson(token.getValue()));
			if (token.getValue().isJsonObject()) {
				Map<String, Object> subjson = new HashMap<>();
				subjson = flattenElements(subjson, token.getValue().getAsJsonObject().entrySet().iterator(), delim);
				target = mergeMaps(target, subjson, token.getKey(), delim);
			} else if (token.getValue().isJsonArray()) {
				JsonArray ja = token.getValue().getAsJsonArray();
				boolean array = false;
				for (int jaIndex = 0; jaIndex < ja.size(); jaIndex++) {
					try{
						JsonObject json = prependIndexToNames(ja.get(jaIndex).getAsJsonObject().entrySet(), jaIndex, delim);
						ja.set(jaIndex, flattenJson(json, delim));						
					}catch(IllegalStateException e){
						array = true;
						break;
					}
				}
				if(!array){
				for (int jaIndex = 0; jaIndex < ja.size(); jaIndex++) {
					Set<Entry<String, JsonElement>> jObj = ja.get(jaIndex).getAsJsonObject().entrySet();
					for (Entry<String, JsonElement> entry : jObj) {
						target.put(token.getKey() + delim + entry.getKey(), entry.getValue());
					}
				}
				} else {
					target.put(token.getKey(), ja);
				}
			} else {
				target.put(token.getKey(), token.getValue());
			}
		}
		return target;
	}

	private JsonObject prependIndexToNames(Set<Entry<String, JsonElement>> entrySet, int jaIndex, String delim) {
		JsonObject json = new JsonObject();
		for (Entry<String, JsonElement> entry : entrySet) {
			json.add(jaIndex + delim + entry.getKey(), entry.getValue());
		}
		return json;
	}

	private Map<String, Object> mergeMaps(Map<String, Object> target, Map<String, Object> source, String prepender,
			String delim) {
		Iterator<Entry<String, Object>> it = source.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> next = it.next();
			target.put(prepender + delim + next.getKey(), next.getValue());
		}
		return target;
	}

}
