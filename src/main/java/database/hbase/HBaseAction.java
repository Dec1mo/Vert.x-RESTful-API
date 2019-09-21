package database.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableExistsException;

import config.Constants;
import io.vertx.core.json.JsonObject;

public class HBaseAction {
	private HTable userTable = null;
	private static HBaseAction hbAction;

	@SuppressWarnings("deprecation")
	public HBaseAction() {
		try {
			this.createHBaseTable("users", "Names", "Years");
		} catch (TableExistsException e) {
			System.out.println("TableName existed");
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("IOException");
		}
		try {
			this.userTable = new HTable(HBaseConnection.getInstance().getConfiguration(),
					Constants.USERS_TABLE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Can't get HTable");
		}
		;
	}

	public void createHBaseTable(String name, String... families) throws IOException {
		TableName tableName = TableName.valueOf(name);
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		for (String family : families) {
			descriptor.addFamily(new HColumnDescriptor(family));
		}
		System.out.println(descriptor);
		HBaseConnection.getInstance().getAdmin().createTable(descriptor);
	}

	public void insertUser(JsonObject jsonUser) {
		byte[] row = Bytes.toBytes(jsonUser.getInteger("id"));
		Put put = new Put(row);
		put.addImmutable(Constants.USER_TABLE_FAMILY1, Constants.USER_TABLE_F1_QUALIFIER1,
				jsonUser.getString("name").getBytes());
		put.addImmutable(Constants.USER_TABLE_FAMILY2, Constants.USER_TABLE_F2_QUALIFIER1,
				jsonUser.getString("year").getBytes());
		try {
			this.userTable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cant put to table!!!");
		}
	}

	public ArrayList<JsonObject> getAllUsers() {
		Scan scan = new Scan();

		scan.addColumn(Constants.USER_TABLE_FAMILY1, Constants.USER_TABLE_F1_QUALIFIER1);
		scan.addColumn(Constants.USER_TABLE_FAMILY2, Constants.USER_TABLE_F2_QUALIFIER1);

		ArrayList<JsonObject> users = new ArrayList<JsonObject>();
		try {
			ResultScanner scanner = this.userTable.getScanner(scan);

			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				JsonObject jsonUser = new JsonObject();
				jsonUser.put("id", Bytes.toInt(result.getRow()));
				jsonUser.put("name", Bytes.toString(result.getValue(
						Constants.USER_TABLE_FAMILY1, 
						Constants.USER_TABLE_F1_QUALIFIER1)));
				jsonUser.put("year", Bytes.toString(result.getValue(
						Constants.USER_TABLE_FAMILY2, 
						Constants.USER_TABLE_F2_QUALIFIER1)));
				users.add(jsonUser);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return users;
	}

	public void deleteUserById(String id) {
		Delete delete = new Delete(Bytes.toBytes(id));
		delete.addColumn(Constants.USER_TABLE_FAMILY1,
				Constants.USER_TABLE_F1_QUALIFIER1);
		delete.addColumn(Constants.USER_TABLE_FAMILY2,
				Constants.USER_TABLE_F2_QUALIFIER1);
		try {
			this.userTable.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static HBaseAction getInstance() {
		if (hbAction == null) {
			hbAction = new HBaseAction();
		}
		return hbAction;
	}

}
