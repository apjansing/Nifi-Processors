package com.apjansing;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apjansing.JsonCompresser;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CompresserTest {

	static Gson gson = new Gson();
	static JsonParser jp = new JsonParser();

	public static void main(String[] args) {
		getJson("sampleJsons/basic_flattened.json");
		getJson("sampleJsons/arrays_flattened.json");
		getJson("sampleJsons/jsonArrayWithArrays_flattened.json");
		getJson("sampleJsons/complex_flattened.json");
	}

	private static void getJson(String ressource) {
		JsonElement elem = jp.parse(new CompresserTest().getFile(ressource));
		Logger logger = LoggerFactory.getLogger(CompresserTest.class);
		if (elem.isJsonObject()) {
			JsonObject json = elem.getAsJsonObject();
			System.out.println(json);
			JsonCompresser J = new JsonCompresser(json, ".", logger);
			System.out.println(J.getJson());
		} else {
			JsonArray json = elem.getAsJsonArray();
			JsonArray ja = new JsonCompresser(jp.parse(new CompresserTest().getFile(ressource)).getAsJsonArray(), ".",
					logger).getJsonArray();
			System.out.println(json);
			System.out.println(ja);
		}
		System.out.println("-------------");
	}

	private String getFile(String fileName) {

		StringBuilder result = new StringBuilder("");

		// Get file from resources folder
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());

		try (Scanner scanner = new Scanner(file)) {
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				result.append(line).append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result.toString();
	}

}
