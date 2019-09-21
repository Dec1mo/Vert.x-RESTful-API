package dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import config.Constants;
import database.aerospike.AerospikeAction;
import database.hbase.HBaseAction;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import manager.UserManager;

public class UserDAO {

	private static UserDAO userDAO;
	private static boolean isAerospikeEnabled = false;
	private static boolean isHBaseEnabled = true;
	LinkedHashMap<Integer, JsonObject> users = new LinkedHashMap<Integer, JsonObject>();

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

		HBaseAction hBaseAction = new HBaseAction();
		hBaseAction.insertUser(userToJson(user1));
		hBaseAction.insertUser(userToJson(user2));
		hBaseAction.insertUser(userToJson(user3));

//		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
//				user1, Constants.NAMESPACE, Constants.SETNAME);
//		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
//				user2, Constants.NAMESPACE, Constants.SETNAME);
//		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
//				user3, Constants.NAMESPACE, Constants.SETNAME);
//		users.put(user1.getId(), userToJson(user1));
//		users.put(user2.getId(), userToJson(user2));
//		users.put(user3.getId(), userToJson(user3));
	}

	// ok
	public void initData() {
		ArrayList<JsonObject> usersList = null;
		if (isAerospikeEnabled) {
			usersList = AerospikeAction.getInstance().getAllUsers();
		} else if (isHBaseEnabled) {
			usersList = HBaseAction.getInstance().getAllUsers();
		}
		int maxId = Collections.max(usersList, new Comparator<JsonObject>() {
			public int compare(JsonObject j1, JsonObject j2) {
				return Integer.compare(j1.getInteger("id"), j1.getInteger("id"));
			}
		}).getInteger("id");
		UserManager.COUNTER.addAndGet(maxId);
		for (JsonObject jsonUser : usersList) {
			users.put(jsonUser.getInteger("id"), jsonUser);
		}
	}

	// ok
	public void getAll(RoutingContext routingContext) {
		ArrayList<JsonObject> usersList = null;
		if (isAerospikeEnabled) {
			usersList = AerospikeAction.getInstance().getAllUsers();
		}
		if (isHBaseEnabled) {
			usersList = HBaseAction.getInstance().getAllUsers();
		}
		for (JsonObject jsonUser : usersList) {
			users.put(jsonUser.getInteger("id"), jsonUser);
		}
		routingContext.response().putHeader("context-type", "application/json; charset=utf-8")
				.end(Json.encodePrettily(users.values()));
	}

	// ok
	public void addOne(RoutingContext routingContext) {
		final UserManager user = Json.decodeValue(routingContext.getBodyAsString(), UserManager.class);
		JsonObject jsonUser = userToJson(user);
		if (isAerospikeEnabled) {
			AerospikeAction.getInstance().putUser(jsonUser, Constants.NAMESPACE, Constants.SETNAME);
		}
		if (isHBaseEnabled) {
			HBaseAction.getInstance().insertUser(jsonUser);
		}
		users.put(user.getId(), jsonUser);
		routingContext.response().setStatusCode(201).putHeader("content-type", "application/json; charset=utf-8")
				.end(Json.encodePrettily(user));
	}

	// ok
	public void deleteOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		if (id == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			Integer idAsInteger = Integer.valueOf(id);
			if (isAerospikeEnabled) {
				AerospikeAction.getInstance().deleteUserById(idAsInteger);
			}
			if (isHBaseEnabled) {
				HBaseAction.getInstance().deleteUserById(id);
			}
			users.remove(idAsInteger);
		}
		routingContext.response().setStatusCode(204).end();
	}

	// ok
	public void updateOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		JsonObject json = routingContext.getBodyAsJson();
		if (id == null || json == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			final Integer idInteger = Integer.valueOf(id);
			JsonObject jsonUser = users.get(idInteger);
			if (jsonUser == null) {
				routingContext.response().setStatusCode(404).end();
			} else {
				jsonUser.put("name", json.getString("name"));
				jsonUser.put("year", Integer.valueOf(json.getString("year")));
				
				if (isAerospikeEnabled) {
					AerospikeAction.getInstance().putUser(jsonUser, Constants.NAMESPACE, Constants.SETNAME);
				}
				if (isHBaseEnabled) {
					HBaseAction.getInstance().insertUser(jsonUser);
				}
				routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
						.end(Json.encodePrettily(jsonUser));
			}
		}
	}

	// dont need to get from aerospike database or hbase database
	// get from linkedhashmap instead
	public void getOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		if (id == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			Integer idInteger = Integer.valueOf(id);
			JsonObject jsonUser = users.get(idInteger);
			if (jsonUser == null) {
				routingContext.response().setStatusCode(404).end();
			} else {
				routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
						.end(Json.encodePrettily(jsonUser));
			}
		}
	}

	public static UserDAO getInstance() {
		if (userDAO == null) {
			userDAO = new UserDAO();
		}
		return userDAO;
	}

}
