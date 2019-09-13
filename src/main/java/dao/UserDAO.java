package dao;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import config.Constants;
import database.aerospike.AerospikeConnection;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import manager.UserManager;

public class UserDAO {

	private static UserDAO userDAO = new UserDAO();

	LinkedHashMap users = new LinkedHashMap<Integer, JsonObject>();
	
	private void putToAerospike(WritePolicy writePolicy, AerospikeClient client, 
			UserManager user, String namespace,	String setName) {
		Key key = new Key(namespace, setName, user.getId());
		Bin[] bins = { new Bin("name", user.getName()), 
				new Bin("id", user.getId()),
				new Bin("year", user.getYear()) };
		client.put(writePolicy, key, bins);
	}
	
	private void putToAerospike(WritePolicy writePolicy, AerospikeClient client, 
			JsonObject jsonUser, String namespace,	String setName) {
		Key key = new Key(namespace, setName, jsonUser.getInteger("id"));
		Bin[] bins = { new Bin("name", jsonUser.getString("name")), 
				new Bin("id", jsonUser.getInteger("id")),
				new Bin("year", jsonUser.getInteger("year")) };
		client.put(writePolicy, key, bins);
	}
	
	private JsonObject userToJson(UserManager user) {
		JsonObject jsonUser = new JsonObject();
		
		jsonUser.put("id", user.getId());
		jsonUser.put("name", user.getName());
		jsonUser.put("year", user.getYear());
		
		return jsonUser;
	}

	public void createSomeData() {
		UserManager user1 = new UserManager("Do Duc Thai", 1998);
		UserManager user2 = new UserManager("Bui Hong Ngoc", 1998);
		UserManager user3 = new UserManager("Le Quang Linh", 1998);
		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
				user1, Constants.NAMESPACE, Constants.SETNAME);
		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
				user2, Constants.NAMESPACE, Constants.SETNAME);
		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
				user3, Constants.NAMESPACE, Constants.SETNAME);
		users.put(user1.getId(), userToJson(user1));
		users.put(user2.getId(), userToJson(user2));
		users.put(user3.getId(), userToJson(user3));
	}

	public void getAll(RoutingContext routingContext) {
		Statement stmt = new Statement();
		stmt.setNamespace(Constants.NAMESPACE);
		stmt.setSetName(Constants.SETNAME);
		
		RecordSet rs = AerospikeConnection.getAerospikeClient().query(null, stmt);
		
		try {
		    while (rs.next()) {
		    	JsonObject jsonBin = new JsonObject();
//		        Key key = rs.getKey();
		        Record re = rs.getRecord();
		    	jsonBin.put("id", re.getInt("id"));
		    	jsonBin.put("name", re.getString("name"));
		    	jsonBin.put("year", re.getInt("year"));
		    	
		    	users.put(re.getInt("id"), jsonBin);
		    }
		}
		finally {
		    rs.close();
		}
		
		routingContext.response().putHeader("context-type", "application/json; charset=utf-8")
				.end(Json.encodePrettily(users.values()));
	}

	public void addOne(RoutingContext routingContext) {
		final UserManager user = Json.decodeValue(routingContext.getBodyAsString(), UserManager.class);
		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
				user, Constants.NAMESPACE, Constants.SETNAME);
		users.put(user.getId(), user);
		routingContext.response().setStatusCode(201).putHeader("content-type", "application/json; charset=utf-8")
				.end(Json.encodePrettily(user));
	}

	public void deleteOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		if (id == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			Integer idAsInteger = Integer.valueOf(id);
			Key key = new Key(Constants.NAMESPACE, Constants.SETNAME, idAsInteger);
			AerospikeConnection.getAerospikeClient().delete(null, key);
			users.remove(idAsInteger);
		}
		routingContext.response().setStatusCode(204).end();
	}

	public void updateOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		JsonObject json = routingContext.getBodyAsJson();
		if (id == null || json == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			final Integer idInteger = Integer.valueOf(id);
			JsonObject jsonUser = (JsonObject) users.get(idInteger);
			System.out.println(jsonUser);
			if (jsonUser == null) {
				routingContext.response().setStatusCode(404).end();
			} else {
				jsonUser.put("name", json.getString("name"));
				jsonUser.put("year", Integer.valueOf(json.getString("year")));
				putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
						jsonUser, Constants.NAMESPACE, Constants.SETNAME);
				routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
						.end(Json.encodePrettily(jsonUser));
			}
		}
	}

	public void getOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		if (id == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			Integer idInteger = Integer.valueOf(id);
			JsonObject jsonUser = (JsonObject) users.get(idInteger);
			if (jsonUser == null) {
				routingContext.response().setStatusCode(404).end();
			} else {
				routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
						.end(Json.encodePrettily(jsonUser));
			}
		}
	}

	public static UserDAO getUserDAO() {
		return userDAO;
	}

}
