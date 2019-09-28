package dao;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import config.Constants;
import database.aerospike.AerospikeAction;
import database.hbase.HBaseAction;
import database.mysql.MySQLAction;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import manager.UserManager;

public class UserDAO {

	private static UserDAO userDAO;

	// just simulate scenarios
	private static boolean isAerospikeEnabled = false;
	private static boolean isHBaseEnabled = false;
	private static boolean isMySQLEnabled = true;
	LinkedHashMap<Integer, JsonObject> users = new LinkedHashMap<Integer, JsonObject>();

	private JsonObject userToJson(UserManager user) {
		JsonObject jsonUser = new JsonObject();

		jsonUser.put("id", user.getId());
		jsonUser.put("name", user.getName());
		jsonUser.put("year", user.getYear());

		return jsonUser;
	}

//	public void createSomeData() {
//		UserManager user1 = new UserManager("Do Duc Thai", 1998);
//		UserManager user2 = new UserManager("Bui Hong Ngoc", 1998);
//		UserManager user3 = new UserManager("Le Quang Linh", 1998);
//
//		HBaseAction.getInstance().insertUser(userToJson(user1));
//		HBaseAction.getInstance().insertUser(userToJson(user2));
//		HBaseAction.getInstance().insertUser(userToJson(user3));
//
////		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
////				user1, Constants.NAMESPACE, Constants.SETNAME);
////		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
////				user2, Constants.NAMESPACE, Constants.SETNAME);
////		putToAerospike(new WritePolicy(), AerospikeConnection.getAerospikeClient(), 
////				user3, Constants.NAMESPACE, Constants.SETNAME);
////		users.put(user1.getId(), userToJson(user1));
////		users.put(user2.getId(), userToJson(user2));
////		users.put(user3.getId(), userToJson(user3));
//	}

	// ok
	public void initData() {
		ArrayList<JsonObject> usersList = null;
		try {
			if (isAerospikeEnabled) {
				usersList = AerospikeAction.getInstance().getAllUsers();
			} else if (isHBaseEnabled) {
				usersList = HBaseAction.getInstance().getAllUsers();
			} else if (isMySQLEnabled) {
				usersList = MySQLAction.getInstance().getAllUsers();
			}
		} catch (IOException | SQLException e) {
			e.printStackTrace();
			return;
		}
		int maxId = -1;
		if (!usersList.isEmpty()) {
			maxId = Collections.max(usersList, new Comparator<JsonObject>() {
				public int compare(JsonObject j1, JsonObject j2) {
					return Integer.compare(j1.getInteger("id"), j2.getInteger("id"));
				}
			}).getInteger("id");
		}
		System.out.println(maxId + 1);
		UserManager.COUNTER.addAndGet(maxId + 1);
		for (JsonObject jsonUser : usersList) {
			users.put(jsonUser.getInteger("id"), jsonUser);
		}
	}

	// ok
	public void getAll(RoutingContext routingContext) {
		ArrayList<JsonObject> usersList = null;
		try {
			if (isAerospikeEnabled) {
				usersList = AerospikeAction.getInstance().getAllUsers();
			} else if (isHBaseEnabled) {
				usersList = HBaseAction.getInstance().getAllUsers();
			} else if (isMySQLEnabled) {
				usersList = MySQLAction.getInstance().getAllUsers();
			}
		} catch (IOException | SQLException e) {
			e.printStackTrace();
			return;
		}
		for (JsonObject jsonUser : usersList) {
			users.put(jsonUser.getInteger("id"), jsonUser);
		}
		routingContext.response().putHeader("context-type", "application/json; charset=utf-8")
				.end(Json.encodePrettily(users.values()));
		System.out.println(users.values());
	}

	// ok
	public void addOne(RoutingContext routingContext) {
		System.out.println(routingContext.getBodyAsString());
		JsonObject jsonUserData = new JsonObject(routingContext.getBodyAsString());
		UserManager user = null;
		try {
			user = new UserManager(jsonUserData.getString("name"), Integer.valueOf(jsonUserData.getString("year")));
		} catch (NumberFormatException e) {
			System.out.println("Year must be integer!!");
		}
//		System.out.println(user.getId());
		JsonObject jsonUser = userToJson(user);
		// Need some mechanisms to handle more than these things
		try {
			if (isAerospikeEnabled) {
				AerospikeAction.getInstance().putUser(jsonUser, Constants.NAMESPACE, Constants.SETNAME);
			}
			if (isHBaseEnabled) {
				HBaseAction.getInstance().insertUser(jsonUser);
			}
			if (isMySQLEnabled) {
				MySQLAction.getInstance().insertUser(jsonUser);
			}
		} catch (IOException | SQLException e) {
			e.printStackTrace();
			return;
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
			try {
				if (isAerospikeEnabled) {
					AerospikeAction.getInstance().deleteUserById(idAsInteger);
				}
				if (isHBaseEnabled) {
					HBaseAction.getInstance().deleteUserById(idAsInteger);
				}
				if (isMySQLEnabled) {
					MySQLAction.getInstance().deleteUserById(idAsInteger);
				}
			} catch (IOException | SQLException e) {
				e.printStackTrace();
				return;
			}
			users.remove(idAsInteger);
//			System.out.println(users);
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

				// Need some mechanisms to handle more than these things
				try {
					if (isAerospikeEnabled) {
						AerospikeAction.getInstance().putUser(jsonUser, Constants.NAMESPACE, Constants.SETNAME);
					}
					if (isHBaseEnabled) {
						HBaseAction.getInstance().insertUser(jsonUser);
					}
					if (isMySQLEnabled) {
						MySQLAction.getInstance().updateUser(jsonUser);
					}
				} catch (IOException | SQLException e) {
					e.printStackTrace();
					return;
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
