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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Tags({ "json", "jsonarray", "flatten", "expand", "ajansing" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class JsonAccordion extends AbstractProcessor {

	final Logger logger = LoggerFactory.getLogger(JsonAccordion.class);
	final NifiTools nt = new NifiTools();
	final Gson gson = new Gson();

	public static final PropertyDescriptor COMPRESSOR_FLATTEN = new PropertyDescriptor.Builder()
			.name("Expand or flatten").allowableValues("Compress", "Flatten").defaultValue("Compress").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

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
		descriptors.add(COMPRESSOR_FLATTEN);
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
			switch (context.getProperty(COMPRESSOR_FLATTEN).getValue()) {
			case "Compress":
				compress(flowFile, session, delim);
				break;
			case "Flatten":
				flatten(flowFile, session, delim);
				break;
			}
		} catch (Exception e) {
			logger.error("Error parsing json.", e);
			session.transfer(flowFile, ERROR);
		}
	}

	private void compress(FlowFile flowFile, ProcessSession session, String delim) {
		String resource = nt.readAsString(flowFile, session);
		JsonParser jp = new JsonParser();
		JsonElement elem = jp.parse(resource);

		if (elem.isJsonObject()) {
			JsonObject originalJson = elem.getAsJsonObject();
			JsonObject compressedJson = new JsonCompresser(jp.parse(resource).getAsJsonObject(), delim, logger)
					.getJson();
			done(originalJson, compressedJson, flowFile, session);
		} else {
			JsonArray originalJson = elem.getAsJsonArray();
			JsonArray flattenedJson = new JsonCompresser(jp.parse(resource).getAsJsonArray(), delim, logger)
					.getJsonArray();
			done(originalJson, flattenedJson, flowFile, session);
		}
	}

	private void flatten(FlowFile flowFile, ProcessSession session, String delim) {
		String resource = nt.readAsString(flowFile, session);
		JsonParser jp = new JsonParser();
		JsonElement elem = jp.parse(resource);

		if (elem.isJsonObject()) {
			JsonObject originalJson = elem.getAsJsonObject();
			JsonObject flattenedJson = new JsonFlattener(jp.parse(resource).getAsJsonObject(), ".", logger).getJson();
			done(originalJson, flattenedJson, flowFile, session);
		} else {
			JsonArray originalJson = elem.getAsJsonArray();
			JsonArray flattenedJson = new JsonFlattener(jp.parse(resource).getAsJsonArray(), ".", logger)
					.getJsonArray();
			done(originalJson, flattenedJson, flowFile, session);
		}
	}

	private void done(JsonElement originalJson, JsonElement flattenedJson, FlowFile flowFile, ProcessSession session) {
		FlowFile flat = session.clone(flowFile);
		flowFile = nt.writeFlowFile(flowFile, session, originalJson);
		session.transfer(flowFile, ORIGINAL);
		flat = nt.writeFlowFile(flat, session, flattenedJson);
		session.transfer(flat, FLATTENED);
	}

}
