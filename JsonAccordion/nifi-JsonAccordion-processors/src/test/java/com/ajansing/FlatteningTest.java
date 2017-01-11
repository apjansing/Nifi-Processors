package com.ajansing;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FlatteningTest {

	static Gson gson = new Gson();
	static JsonParser jp = new JsonParser();

	public static void main(String[] args) {
		
		
		getJson("sampleJsons/basic.json");
		getJson("sampleJsons/complex.json");
		getJson("sampleJsons/arrays.json");
		getJson("sampleJsons/jsonArrayWithArrays.json");
		
	}
	
	
	  private static void getJson(String ressource) {
		  JsonElement elem = jp.parse(new FlatteningTest().getFile(ressource));
		  System.out.println(elem);
		  System.out.println(elem);
		  if(elem.isJsonObject()){
			  JsonObject json = elem.getAsJsonObject();			  
			  Logger logger = LoggerFactory.getLogger(FlatteningTest.class);
			  JsonFlattener jf = new JsonFlattener(json, ".", logger);
			  System.out.println(gson.toJson(json));
			  System.out.println(gson.toJson(jf.getJson()));
			  System.out.println("-------------");
		  } else {
			  JsonArray json = elem.getAsJsonArray();
			  String holder = gson.toJson(json);
			  Logger logger = LoggerFactory.getLogger(FlatteningTest.class);
			  JsonFlattener jf = new JsonFlattener(json, ".", logger);
			  System.out.println(json);			  
			  System.out.println(gson.toJson(jf.getJsonArray()) +" ME");
			  System.out.println("-------------");
		  }
		
	}


	private String getFile(String fileName) {

			StringBuilder result = new StringBuilder("");

			//Get file from resources folder
			ClassLoader classLoader = getClass().getClassLoader();
			File file = new File(classLoader.getResource(fileName).getFile());

			try (Scanner scanner = new Scanner(file)) {

				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					result.append(line).append("\n");
				}

				scanner.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

			return result.toString();

		  }
	
}
