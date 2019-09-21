package database.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import config.Constants;

public class HBaseConnection {
	private Configuration configuration;
	private Connection connection = null;
	private Admin admin = null;
	
	private static HBaseConnection hbaseConnection;
	
	public HBaseConnection(){
		this.configuration = HBaseConfiguration.create();
		this.configuration.addResource(new Path(Constants.HBASE_SITE));
		try {
			this.connection = ConnectionFactory.createConnection(this.configuration);
			this.admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Can't connect");
		}
	}
	
	public static HBaseConnection getInstance() {
		if (hbaseConnection == null) {
			hbaseConnection = new HBaseConnection();
		}
		return hbaseConnection;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public Admin getAdmin() {
		return admin;
	}

	public void setAdmin(Admin admin) {
		this.admin = admin;
	}
	
	
}
