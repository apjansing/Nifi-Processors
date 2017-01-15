package com.ajansing;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CompresserTest {

	static Gson gson = new Gson();
	static JsonParser jp = new JsonParser();

	public static void main(String[] args) {
		
//		Set<Entry<String, Integer>> set = new HashSet<Entry<String, Integer>>();
//		Set<Entry<String, Integer>> otherSet = new HashSet<Entry<String, Integer>>();
//		
//		set.add(new MyEntry<String, Integer>("one", 1));
//		set.add(new MyEntry<String, Integer>("two", 2));
//		set.add(new MyEntry<String, Integer>("three", 3));
//		set.add(new MyEntry<String, Integer>("four", 4));
//		
//		otherSet.add(new MyEntry<String, Integer>("two", 2));
//		otherSet.add(new MyEntry<String, Integer>("three", 3));
//		
//outer:		for(Entry<String, Integer> s: set){
//			for(Entry<String, Integer> o : otherSet){				
//				if(o.getKey() == s.getKey()){
//					System.out.println("wer");
//					set.remove(s);
//					break outer;
//				}
//			}
//		}
//		
//		Iterator<Entry<String, Integer>> it = set.iterator();
//		while(it.hasNext()){
//			System.out.println(it.next().getKey());
//		}
		
		getJson("sampleJsons/basic_flattened.json");
		getJson("sampleJsons/arrays_flattened.json");
		getJson("sampleJsons/jsonArrayWithArrays_flattened.json");
		getJson("sampleJsons/complex_flattened.json");

		
	}
	
	
	  private static void getJson(String ressource) {
		  JsonElement elem = jp.parse(new CompresserTest().getFile(ressource));
		  Logger logger = LoggerFactory.getLogger(CompresserTest.class);
		  if(elem.isJsonObject()){
			  JsonObject json = elem.getAsJsonObject();
			  System.out.println(json);
			  JsonCompresser J = new JsonCompresser(json, ".", logger);
			  System.out.println(J.getJson());
		  } else {
			  JsonArray json = elem.getAsJsonArray();
			  JsonArray ja = new JsonFlattener(jp.parse(new CompresserTest().getFile(ressource)).getAsJsonArray(), ".", logger).getJsonArray();
			  System.out.println(json);			  
			  System.out.println(ja);
		  }
		  System.out.println("-------------");
		
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
