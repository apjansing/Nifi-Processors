package com.apjansing;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author apjansing
 *
 */
public class JsonCompresser {
	
	private Logger logger;
	final Gson gson = new Gson();
	private static JsonObject json;
	private static JsonArray jsonArray;
	private static String delim;

	protected JsonCompresser(JsonArray originalJsonArray, String delim, Logger log) {
		setLog(log);
		setDelim(delim);
		setJsonArray(compressFlattenedJsonArray(originalJsonArray, delim, new JsonObject()));
	}

	protected JsonCompresser(JsonObject originalJson, String delim, Logger log) {
		setLog(log);
		setDelim(delim);
		setJson(compressFlattenedJson(originalJson.entrySet(), new JsonObject()));
	}

	private JsonArray compressFlattenedJsonArray(JsonArray originalJsonArray, String delim, JsonObject jsonObject) {
		for (int i = 0; i < originalJsonArray.size(); i++) {
			originalJsonArray.set(i,
					compressFlattenedJson(originalJsonArray.get(i).getAsJsonObject().entrySet(), new JsonObject()));
		}
		return originalJsonArray;
	}

	private JsonObject compressFlattenedJson(Set<Entry<String, JsonElement>> entrySet, JsonObject masterJson) {
		Set<Entry<String, JsonElement>> group = new HashSet<>();
		Iterator<Entry<String, JsonElement>> it = entrySet.iterator();
		String currentSubJsonKey = "";
		while (it.hasNext()) {
			Entry<String, JsonElement> next = it.next();
			if (group.size() < 1) {
				currentSubJsonKey = next.getKey().split(Pattern.quote(delim))[0];
				group.add(next);
			} else {
				String[] key = next.getKey().split(Pattern.quote(delim));
				// logger.info("Current SubJson Key " + currentSubJsonKey);
				if (key[0].equals(currentSubJsonKey)) {
					group.add(next);
				}
			}
		}

		JsonElement currentSubJson = setToJsonObject(group);

		if (checkForNesting(currentSubJson.getAsJsonObject())) {
			currentSubJson = compressFlattenedJson(currentSubJson.getAsJsonObject().entrySet(), new JsonObject());
		}

		if (jsonArrayP(currentSubJson.getAsJsonObject())) {
			currentSubJson = toJsonArray(currentSubJson);
		}

		masterJson.add(currentSubJsonKey, currentSubJson);

		logger.info(gson.toJson(masterJson));
		entrySet = removeSubset(entrySet, group);
		if (group.size() == 1 && !group.iterator().next().getKey().contains(delim)) {
			Entry<String, JsonElement> token = group.iterator().next();
			masterJson.add(token.getKey(), token.getValue());
		} else {
			if (entrySet.size() > 0 && group.size() != 0) {
				masterJson = compressFlattenedJson(entrySet, masterJson);
			}
		}
		return masterJson;
	}

	private JsonElement toJsonArray(JsonElement currentSubJson) {
		JsonArray ja = new JsonArray();
		Iterator<Entry<String, JsonElement>> it = currentSubJson.getAsJsonObject().entrySet().iterator();
		while (it.hasNext()) {
			ja.add(it.next().getValue());
		}
		return ja;
	}

	private boolean jsonArrayP(JsonObject currentSubJson) {
		Set<Entry<String, JsonElement>> keys = currentSubJson.entrySet();
		for (Entry<String, JsonElement> k : keys) {
			try {
				Integer.parseInt(k.getKey());
			} catch (NumberFormatException | NullPointerException e) {
				return false;
			}
		}
		return true;
	}

	private boolean checkForNesting(JsonObject currentSubJson) {
		Set<Entry<String, JsonElement>> keys = currentSubJson.entrySet();
		for (Entry<String, JsonElement> k : keys) {
			if (k.getKey().contains(delim)) {
				return true;
			}
		}
		return false;
	}

	private Set<Entry<String, JsonElement>> removeSubset(Set<Entry<String, JsonElement>> entrySet,
			Set<Entry<String, JsonElement>> group) {
		for (Entry<String, JsonElement> g : group) {
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

	private JsonObject setToJsonObject(Set<Entry<String, JsonElement>> group) {
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
		this.logger = log;
	}

	public JsonObject getJson() {
		return json;
	}

	public void setJson(JsonObject json) {
		JsonCompresser.json = json;
	}

	public JsonArray getJsonArray() {
		return jsonArray;
	}

	public void setJsonArray(JsonArray jsonArray) {
		JsonCompresser.jsonArray = jsonArray;
	}

	public void setDelim(String delim) {
		JsonCompresser.delim = delim;
	}

	public String getDelim() {
		return delim;
	}

}
