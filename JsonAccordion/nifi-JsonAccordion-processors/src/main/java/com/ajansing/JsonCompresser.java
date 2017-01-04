package com.ajansing;

import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JsonCompresser {

	Logger log;
	final Gson gson = new Gson();
	static private JsonElement json;

	public JsonCompresser(JsonArray originalJsonArray, String delim, Logger log) {
		setLog(log);
		setJson(compressJsonArray(originalJsonArray, delim));
	}

	public JsonCompresser(JsonObject originalJson, String delim, Logger log) {
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
			if(originalJsonArray.isJsonArray()){
				originalJsonArray.set(i, compressJsonArray(originalJsonArray, delim));
			}else {
				JsonObject json = compressJson(originalJsonArray.get(i).getAsJsonObject(), delim);
				originalJsonArray.set(i, json);				
			}
		}
		return originalJsonArray;
	}

	private JsonObject compressJson(JsonObject jsonObject, String delim) {
		Iterator<Entry<String, JsonElement>> it = jsonObject.entrySet().iterator();
		JsonObject json = new JsonObject();
		while (it.hasNext()) {
			Entry<String, JsonElement> token = it.next();
			String key = token.getKey();
			if(key.contains(delim)){			
				String[] K = key.split(delim, 2);
				try{
					int k = Integer.valueOf(K[0]);
					JsonArray ja = new JsonArray();
					ja = makeJsonArray(jsonObject, ja, delim);
					break;
				} catch (NumberFormatException e){					
					JsonObject subJson = new JsonObject();
					subJson.add(K[1], token.getValue());
					json.add(K[0], subJson);
				}
			} else {
				json.add(key, token.getValue());
			}
		}
		return json;
	}

	private JsonArray makeJsonArray(JsonObject jsonObject, JsonArray ja, String delim) {
		Iterator<Entry<String, JsonElement>> it = jsonObject.entrySet().iterator();
		
		String markForRemoval = "";
		
		while(it.hasNext()){
			String k = it.next().getKey().split(delim, 2)[0];
			Iterator<Entry<String, JsonElement>> it2 = jsonObject.entrySet().iterator();
			if(k != markForRemoval){
				JsonObject json = new JsonObject();
				while(it2.hasNext()){
					markForRemoval = k;
					Entry<String, JsonElement> token = it2.next();
					String key = token.getKey();
					String[] K = key.split(delim, 2);
					if(K[0] == k){
						json.add(K[1], token.getValue());
						jsonObject.remove(key);
					}
				}
				ja.add(compressJson(json, delim));
			}
		}
		return ja;
	}

}
