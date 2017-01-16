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
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({ "json", "standard fields", "standardize" })
@CapabilityDescription("This processor standardizes the fields of a Json. "
		+ "Please refer to the README in the original Maven project for a "
		+ "full length description of how to use this processor.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class StandardizeJson extends AbstractProcessor {
	
	Logger logger = LoggerFactory.getLogger(StandardizeJson.class);

	public static final PropertyDescriptor STANDARD = new PropertyDescriptor.Builder().name("Standard's location")
			.displayName("Standard's location")
			.description("Path to the csv with a mapping for expected values to standard values.").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor DELIM = new PropertyDescriptor.Builder().name("Flattening delimeter")
			.description("This is the delimeter to be used to signify where the Json was flattened. i.e. "
					+ "first:{second... turns into first.second when the delimeter is \".\".")
			.defaultValue(".").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("Success").description("Success")
			.build();
	public static final Relationship FAILURE = new Relationship.Builder().name("Failure").description("Failure")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(STANDARD);
		descriptors.add(DELIM);
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

		String stdLoc = context.getProperty(STANDARD).getValue();
		String delim = context.getProperty(DELIM).getValue();
		File file = new File(stdLoc);
		try {
			BufferedReader bin = new BufferedReader(new FileReader(file));
			Pattern[] filenamePatterns = getPatterns(bin, delim);
			String ffFileName = flowFile.getAttribute("filename");
			int patternColumn = findPatternColumn(filenamePatterns, ffFileName);
			if(patternColumn == 0){
				session.transfer(flowFile, FAILURE);
			} else {
				String[][] keys = getKeys(bin, delim, patternColumn);
				
			}
		} catch (IOException e) {
			logger.error("IOException with " + file, e);
		}


	}

	private String[][] getKeys(BufferedReader bin, String delim, int patternColumn) throws IOException {
		HashMap<String, String> keyMap = new HashMap<>();
		String line = "";
		while((line = bin.readLine()) != null){
			String[] lineTokens = line.split(Pattern.quote(delim));
		}
		return null;
	}

	private int findPatternColumn(Pattern[] filenamePatterns, String ffFileName) {
		int patternColumn = 0;
		for(Pattern p : filenamePatterns){
			Matcher matcher = p.matcher(ffFileName);
			if(matcher.find()){
				return patternColumn;
			} else {
				patternColumn++;
			}
		}		return 0;
	}

	private Pattern[] getPatterns(BufferedReader bin, String delim) throws IOException {
		String[] P = bin.readLine().split(Pattern.quote(delim));
		ArrayList<Pattern> patterns = new ArrayList<>();
		for(String p : P){
			patterns.add(Pattern.compile(p));
		}
		return patterns.toArray(new Pattern[0]);
	}
}





























