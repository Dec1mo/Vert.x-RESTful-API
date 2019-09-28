package database.mysql;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import config.Constants;

public class MySQLConnection {

	
	private static Properties properties = null;
	private static HikariDataSource dataSource;
	
	static {
		try {
			properties = new Properties();
			properties.load(new FileInputStream("src/main/java/database.properties"));
		
			dataSource = new HikariDataSource();
			dataSource.setDriverClassName(properties.getProperty(Constants.DB_DRIVER_CLASS));
			
			dataSource.setJdbcUrl(properties.getProperty(Constants.DB_URL));
			dataSource.setUsername(properties.getProperty(Constants.DB_USERNAME));
			dataSource.setPassword(properties.getProperty(Constants.DB_PASSWORD));
			
			dataSource.setMinimumIdle(100);
			dataSource.setMaximumPoolSize(2000);
			dataSource.setAutoCommit(true);
			dataSource.setLoginTimeout(6);
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static DataSource getDataSource() {
		return dataSource;
	}
	
}
