package database.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import config.Constants;

public class AerospikeConnection {
	private static ClientPolicy policy = new ClientPolicy();
	private static final AerospikeClient client = new AerospikeClient(policy, Constants.hosts);
	
	public static AerospikeClient getAerospikeClient() {
		return client;
	}
	
}
