package com.ajansing;

import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JsonCompresser {
	
	Logger log;
	final Gson gson = new Gson();
	static private JsonElement json;

	public JsonCompresser(JsonArray originalJsonArray, String delim, Logger log){
		setLog(log);
		setJson(compressJsonArray(originalJsonArray, delim));
	}
	

	public JsonCompresser(JsonObject originalJson, String delim, Logger log){
		setLog(log);
		setJson(compressJson(originalJson, delim));
	}
	


	private void setLog(Logger log) {
		this.log = log;
	}

	public static JsonElement getJson() {
		return json;
	}

	public static void setJson(JsonElement json) {
		JsonCompresser.json = json;
	}
	
	private JsonElement compressJsonArray(JsonArray originalJsonArray, String delim) {
		for (int i = 0; i < originalJsonArray.size(); i++) {
			JsonObject json = compressJson(originalJsonArray.get(i).getAsJsonObject(), delim);
			originalJsonArray.set(i, json);
		}
		return originalJsonArray;
	}
	
	private JsonObject compressJson(JsonObject jsonObject, String delim) {
		Set<Entry<String, JsonElement>> entries = jsonObject.entrySet();
		
		return null;
	}
	
}
