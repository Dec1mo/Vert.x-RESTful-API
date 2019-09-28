package config;

import com.aerospike.client.Host;

public class Constants {
	// Aerospike
	public static final Host[] hosts = new Host[] {
			new Host("127.0.1.1", 3000),
			new Host("127.0.0.1", 3000)
	};
	public static final String NAMESPACE = "test";
	public static final String SETNAME = "users";
	
	// Hbase
	public static final String HBASE_SITE = "/usr/local/hbase/conf/hbase-site.xml";
	public static final String USERS_TABLE_NAME = "users";
	public static final byte[] USER_TABLE_FAMILY1 = "Names".getBytes();
	public static final byte[] USER_TABLE_FAMILY2 = "Years".getBytes();
	public static final byte[] USER_TABLE_F1_QUALIFIER1 = "name".getBytes();
	public static final byte[] USER_TABLE_F2_QUALIFIER1 = "year".getBytes();
	
	// Hikari Connection Pool - MySQL
	public static final String DB_USERNAME="db.username";
	public static final String DB_PASSWORD="db.password";
	public static final String DB_URL ="db.url";
	public static final String DB_DRIVER_CLASS="driver.class.name";
}
