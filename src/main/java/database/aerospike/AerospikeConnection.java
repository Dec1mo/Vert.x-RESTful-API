package database.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import config.Constants;

public class AerospikeConnection {
	private static ClientPolicy policy;
	private static final AerospikeClient client = new AerospikeClient(policy, Constants.hosts);
	
	public static AerospikeClient getAerospikeClient() {
		if (policy == null) {
			policy = new ClientPolicy();
		}
		return client;
	}
	
}
