package com.apjansing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonFlattener {

	Logger log;
	final Gson gson = new Gson();
	final JsonParser jp = new JsonParser();
	private JsonObject json;
	private JsonArray jsonArray;

	private String delim;

	public JsonFlattener(JsonArray originalJsonArray, String delim, Logger log) {
		setLog(log);
		setDelim(delim);
		setJsonArray(flattenJsonArray(originalJsonArray));
	}

	public JsonFlattener(JsonObject originalJson, String delim, Logger log) {
		setJson(flattenJson(originalJson, delim));
		setLog(log);
	}

	private void setJsonArray(JsonArray jsonArray) {
		this.jsonArray = jsonArray;
	}

	private void setDelim(String delim) {
		this.delim = delim;
	}

	public JsonObject getJson() {
		return json;
	}

	public JsonArray getJsonArray() {
		return jsonArray;
	}

	private void setJson(JsonObject json) {
		this.json = json;
	}

	private void setLog(Logger log) {
		this.log = log;
	}

	protected JsonArray flattenJsonArray(JsonArray originalJsonArray) {
		JsonArray ja = jp.parse(originalJsonArray.toString()).getAsJsonArray(); // fixed
																				// a
																				// prolem
																				// I
																				// have.
																				// Probably
																				// a
																				// more
																				// efficient
																				// way
																				// o
		for (int i = 0; i < originalJsonArray.size(); i++) {
			JsonObject json = flattenJson(originalJsonArray.get(i).getAsJsonObject(), delim);
			ja.set(i, json);
		}
		return ja;
	}

	protected JsonObject flattenJson(JsonObject originalJson, String delim) {
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

	private Map<String, Object> flattenElements(Map<String, Object> target, Iterator<Entry<String, JsonElement>> it,
			String delim) {
		while (it.hasNext()) {
			Entry<String, JsonElement> token = it.next();
			// log.info(token.getKey());
			if (token.getValue().isJsonObject()) {
				Map<String, Object> subjson = new HashMap<>();
				subjson = flattenElements(subjson, token.getValue().getAsJsonObject().entrySet().iterator(), delim);
				target = mergeMaps(target, subjson, token.getKey(), delim);
			} else if (token.getValue().isJsonArray()) {
				JsonArray ja = token.getValue().getAsJsonArray();
				boolean array = false;
				for (int jaIndex = 0; jaIndex < ja.size(); jaIndex++) {
					try {
						JsonObject json = prependIndexToNames(ja.get(jaIndex).getAsJsonObject().entrySet(), jaIndex,
								delim);
						ja.set(jaIndex, flattenJson(json, delim));
					} catch (IllegalStateException e) {
						array = true;
						break;
					}
				}
				if (!array) {
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
