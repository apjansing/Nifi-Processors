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
package com.apjansing;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajansing.NifiTools;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This processor standardizes the fields of a Json by lists in properties
 * mapping to the property names.
 * 
 * @author apjansing
 */
@Tags({ "json", "standard fields", "standardize" })
@CapabilityDescription("This processor standardizes the fields of a Json. "
		+ "Create attributes with the key being the destination key and the value being a comma-separated list of keys to change.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class StandardizeJson extends AbstractProcessor {
	final Gson gson = new Gson();
	final NifiTools nt = new NifiTools();
	final Logger logger = LoggerFactory.getLogger(StandardizeJson.class);

	public static final Relationship ORIGINAL = new Relationship.Builder().name("Original").description("Original")
			.build();
	public static final Relationship SUCCESS = new Relationship.Builder().name("Success").description("Success")
			.build();
	public static final Relationship FAILURE = new Relationship.Builder().name("Failure").description("Failure")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
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
		
		JsonElement json = getJsonElement(flowFile, session);
		Set<Entry<String, String>> attributeSet = flowFile.getAttributes().entrySet();
		

	}

	private JsonElement getJsonElement(FlowFile flowFile, ProcessSession session) {
		String resource = nt.readAsString(flowFile, session);
		JsonParser jp = new JsonParser();
		JsonElement elem = jp.parse(resource);
		if(elem.isJsonArray())
			return stadardizeJsonArray(elem.getAsJsonArray(), flowFile.getAttributes().entrySet());
		
		return stadardizeJsonObject(elem.getAsJsonObject(), flowFile.getAttributes().entrySet());
	}

	private JsonObject stadardizeJsonObject(JsonObject asJsonObject, Set<Entry<String, String>> entrySet) {
		Set<Entry<String, JsonElement>> jsonSet = asJsonObject.entrySet();
		for(Entry<String, String> entry : entrySet){
			Set<String> sourceKeys = makeSourceSet(entry.getValue().split(","));
			for(String key : sourceKeys){
				if(asJsonObject.has(key)){
					if(asJsonObject.has(entry.getKey())){
						JsonArray ja = append(asJsonObject.get(entry.getKey()).getAsJsonArray().iterator(), asJsonObject.get(key).getAsJsonArray().iterator());
						asJsonObject.remove(entry.getKey());
						asJsonObject.remove(key);
						asJsonObject.add(entry.getKey(), ja);
					}else{
						asJsonObject.add(entry.getKey(), asJsonObject.get(key));
						asJsonObject.remove(key);
					}
				}
			}
		}
		return asJsonObject;
	}

	private JsonArray append(Iterator<JsonElement> iterator, Iterator<JsonElement> iterator2) {
		JsonArray ja = new JsonArray();
		while(iterator.hasNext())
			ja.add(iterator.next());
		while(iterator2.hasNext())
			ja.add(iterator2.next());
		return ja;
	}

	private Set<String> makeSourceSet(String[] split) {
		Set<String> set = new HashSet<>();
		for(String s : split){
			set.add(s);
		}
		return set;
	}

	private JsonElement stadardizeJsonArray(JsonArray asJsonArray, Set<Entry<String, String>> entrySet) {
		for(int i = 0; i < asJsonArray.size(); i++)
			asJsonArray.set(i, stadardizeJsonObject(asJsonArray.get(i).getAsJsonObject(), entrySet));
		return asJsonArray;
	}
}