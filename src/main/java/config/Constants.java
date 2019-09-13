package config;

import com.aerospike.client.Host;

public class Constants {
	public static final Host[] hosts = new Host[] {
			new Host("127.0.1.1", 3000),
			new Host("127.0.0.1", 3000)
	};
	public static final String NAMESPACE = "test";
	public static final String SETNAME = "users";
	
}
