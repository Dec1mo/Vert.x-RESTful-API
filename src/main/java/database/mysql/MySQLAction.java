package database.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import io.vertx.core.json.JsonObject;

public class MySQLAction {
//	public void createHBaseTable();
	private Connection conn = null;
	private Statement statement = null;

	private static MySQLAction mySQLAction = null;

	public MySQLAction() {
		try {
			conn = MySQLConnection.getDataSource().getConnection();
			statement = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertUser(JsonObject jsonUser) throws SQLException {
		String query = "INSERT INTO kinghub.users (id, name, year) VALUES (?, ?, ?)";
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		preparedStmt.setInt(1, jsonUser.getInteger("id"));
		preparedStmt.setString(2, jsonUser.getString("name"));
		preparedStmt.setInt(3, jsonUser.getInteger("year"));

		preparedStmt.execute();
	}

	public void updateUser(JsonObject jsonUser) throws SQLException {
		String query = "UPDATE kinghub.users SET name = ?, year = ? WHERE id = ?";
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		preparedStmt.setString(1, jsonUser.getString("name"));
		preparedStmt.setInt(2, jsonUser.getInteger("year"));
		preparedStmt.setInt(3, jsonUser.getInteger("id"));

		preparedStmt.executeUpdate();
	}

	public ArrayList<JsonObject> getAllUsers() throws SQLException {
		String query = "SELECT * FROM kinghub.users";
		ResultSet rs = null;
		ArrayList<JsonObject> users = null;
		rs = statement.executeQuery(query);
		users = new ArrayList<JsonObject>();
		while (rs.next()) {
			JsonObject this_user = new JsonObject();
			this_user.put("id", rs.getInt(1));
			this_user.put("name", rs.getString(2));
			this_user.put("year", rs.getInt(3));

			users.add(this_user);
		}
		return users;
	}

	public void deleteUserById(int id) throws SQLException {
		String query = "DELETE FROM kinghub.users WHERE id = ?";
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		preparedStmt.setInt(1, id);

		preparedStmt.execute();

	}

	public static MySQLAction getInstance() {
		if (mySQLAction == null) {
			mySQLAction = new MySQLAction();
		}
		return mySQLAction;
	}
}
