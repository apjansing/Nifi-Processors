package com.ajansing;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JsonCompresser {

	Logger log;
	final Gson gson = new Gson();
	private static JsonObject json;
	private static JsonArray jsonArray;

	public JsonCompresser(JsonArray originalJsonArray, String delim, Logger log) {
		setLog(log);
		setJsonArray(originalJsonArray);
	}

	public JsonCompresser(JsonObject originalJson, String delim, Logger log) {
		setLog(log);
		setJson(originalJson);
		setJson(compressFlattenedJson(originalJson.entrySet(), delim, new JsonObject()));
	}

	private JsonObject compressFlattenedJson(Set<Entry<String, JsonElement>> entrySet, String delim,
			JsonObject masterJson) {
		log.info(String.valueOf(entrySet.size()));
		
		Set<Entry<String, JsonElement>> group = new HashSet<>();
		Iterator<Entry<String, JsonElement>> it = entrySet.iterator();
		String currentSubJsonKey = "";
		while (it.hasNext()) {
			Entry<String, JsonElement> next = it.next();
			if (group.size() <= 1) {
				log.info(next.getKey() + " " + delim);
				log.info(String.valueOf(next.getKey().contains(delim)));
				log.info(String.valueOf(next.getKey().split(Pattern.quote(delim)).length));

				currentSubJsonKey = next.getKey().split(Pattern.quote(delim))[0];
				try{
					group.add(next);				
				} catch(NoSuchElementException e){
					break;
				}
			} else {
				String[] key = next.getKey().split(Pattern.quote(delim));
				log.info("Current SubJson Key " + currentSubJsonKey);
				if (key[0].equals(currentSubJsonKey)) {
					log.info("MATCHED " + next.getKey());
					group.add(next);
				}
			}
		}
		
		log.info("=====");
		printSet(group);
		log.info("=====");
		
		JsonObject currentSubJson = setToJsonObject(group, delim);
		
		log.info(gson.toJson(currentSubJson));
		
		masterJson.add(currentSubJsonKey, currentSubJson);

		log.info(gson.toJson(masterJson));
		entrySet = removeSubset(entrySet, group);

		if (entrySet.size() > 0 && group.size() != 0) {
			masterJson = compressFlattenedJson(entrySet, delim, masterJson);
		}
		return masterJson;

	}

	private void printSet(Set<Entry<String, JsonElement>> group) {
		for(Entry<String, JsonElement> g : group)
			log.info(g.getKey() + " " + g.getValue());
	}

	private Set<Entry<String, JsonElement>> removeSubset(Set<Entry<String, JsonElement>> entrySet,
			Set<Entry<String, JsonElement>> group) {
		for (Entry<String, JsonElement> g : group) {
			log.info(g.getKey());
			entrySet = checkAndRemove(g, entrySet);
		}
		return entrySet;
	}

	private Set<Entry<String, JsonElement>> checkAndRemove(Entry<String, JsonElement> g,
			Set<Entry<String, JsonElement>> entrySet) {
		for (Entry<String, JsonElement> e : entrySet) {
			if (e.getKey() == g.getKey()) {
				entrySet.remove(e);
				return entrySet;
			}
		}
		return entrySet;
	}

	private JsonObject setToJsonObject(Set<Entry<String, JsonElement>> group, String delim) {
		JsonObject J = new JsonObject();
		for (Entry<String, JsonElement> g : group) {
			if (g.getKey().contains(delim)) {
				String[] G = g.getKey().split(Pattern.quote(delim), 2);
				J.add(G[1], g.getValue());
			}
		}
		return J;
	}

	private void setLog(Logger log) {
		this.log = log;
	}

	public static JsonObject getJson() {
		return json;
	}

	public static void setJson(JsonObject json) {
		JsonCompresser.json = json;
	}

	public static JsonArray getJsonArray() {
		return jsonArray;
	}

	public static void setJsonArray(JsonArray jsonArray) {
		JsonCompresser.jsonArray = jsonArray;
	}

}
