package database.janusgraph;


import java.util.ArrayList;
import java.util.Arrays;

import javax.sound.sampled.LineListener;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import com.netflix.astyanax.shaded.org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import com.sun.tools.javac.util.List;

import io.vertx.core.json.JsonObject;


public class JanusGraphAction {
	
	static JanusGraphAction instance;
	
	public void insertUser(JsonObject jsonUser) {
		JanusGraphConnection.getInstance().g.addV("user")
		.property("id", jsonUser.getInteger("id"))
		.property("name", jsonUser.getString("name"))
		.property("year", jsonUser.getInteger("year"))
		.next();
		
		JanusGraphConnection.getInstance().g.tx().commit();
	}
	
	public void updateUser(JsonObject jsonUser) {
		JanusGraphConnection.getInstance().g.V()
		.hasLabel("user")
		.has("id", jsonUser.getInteger("id"))
		.property("name", jsonUser.getString("name"))
		.property("year", jsonUser.getInteger("year"))
		.next();
		
		JanusGraphConnection.getInstance().g.tx().commit();
	}
	
	public ArrayList<JsonObject> getAllUsers() {
		ArrayList<Object> users = null;
		ArrayList<JsonObject> jsonUsers = new ArrayList<JsonObject>();
		try {
//			JanusGraphConnection.getInstance().g.tx().commit();
			users = (ArrayList) JanusGraphConnection.getInstance().g.V().hasLabel("user").valueMap().toList();
		} catch (Exception e) {
			System.out.println("No items");
		}
			
		for (Object user : users) {
			JsonObject json_user = new JsonObject();
			String[] info = (user.toString()).split("]|\\[|,");
			json_user.put("id", Integer.parseInt(info[7]));
			json_user.put("name", info[4]);
			json_user.put("year", Integer.parseInt(info[1]));
			jsonUsers.add(json_user);
		}
		
		return jsonUsers;
		
	}
	
	public void deleteUserById(int id) {
		JanusGraphConnection.getInstance().g.V()
				.hasLabel("user")
				.has("id", id)
				.drop()
				.iterate();
		
		JanusGraphConnection.getInstance().g.tx().commit();
	}
	
	public static JanusGraphAction getInstance() {
		if (instance == null) {
			instance = new JanusGraphAction();
		}
		return instance;
	}
}
