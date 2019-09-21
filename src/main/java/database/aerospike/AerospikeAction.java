package database.aerospike;

import java.util.ArrayList;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import config.Constants;
import io.vertx.core.json.JsonObject;

public class AerospikeAction {

	private static AerospikeAction aerospikeAction;
	
	public ArrayList<JsonObject> getAllUsers() {
		Statement stmt = new Statement();
		stmt.setNamespace(Constants.NAMESPACE);
		stmt.setSetName(Constants.SETNAME);

		RecordSet rs = AerospikeConnection.getAerospikeClient().query(null, stmt);
		ArrayList<JsonObject> jsonList = new ArrayList<JsonObject>();
		while (rs.next()) {
			JsonObject jsonBin = new JsonObject();
			Record re = rs.getRecord();
			jsonBin.put("id", re.getInt("id"));
			jsonBin.put("name", re.getString("name"));
			jsonBin.put("year", re.getInt("year"));
			jsonList.add(jsonBin);
		}
		return jsonList;
	}
	
	public void putUser(JsonObject jsonUser, String namespace,	String setName) {
		Key key = new Key(namespace, setName, jsonUser.getInteger("id"));
		Bin[] bins = { new Bin("name", jsonUser.getString("name")), 
				new Bin("id", jsonUser.getInteger("id")),
				new Bin("year", jsonUser.getInteger("year")) };
		AerospikeConnection.getAerospikeClient().put(new WritePolicy(), key, bins);
	}
	
	public void deleteUserById(int id) {
		Key key = new Key(Constants.NAMESPACE, Constants.SETNAME, id);
		AerospikeConnection.getAerospikeClient().delete(null, key);
	}
	
	public static AerospikeAction getInstance() {
		if (aerospikeAction == null) {
			aerospikeAction = new AerospikeAction();
		}
		return aerospikeAction;
	}
}
