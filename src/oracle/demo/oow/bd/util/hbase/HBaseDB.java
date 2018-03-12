package oracle.demo.oow.bd.util.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDB {

	private Connection conn;

	private static class HbaseDBInstance {
		private static final HBaseDB instance = new HBaseDB();
	}

	public static HBaseDB getInstance() {
		return HbaseDBInstance.instance;
	}

	private HBaseDB() {
		// 获取配置类对象
		Configuration conf = HBaseConfiguration.create();
		// 指定zookeeper地址
		conf.set("hbase.zookeeper.quorum", "hadoop");
		// 指定hbase存储目录
		conf.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
		try {
			// 获取数据库连接
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据表名称和列族创建表
	 * 
	 * @param tableName
	 * @param columnFamilies
	 */
	public void createTable(String tableName, String[] columnFamilies) {
		deleteTable(tableName);
		try {
			Admin admin = conn.getAdmin();
			// 指定表名称
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			// 添加列族
			for (String string : columnFamilies) {
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(string));
				// family.setMaxVersions(maxVersions);
				descriptor.addFamily(family);
			}
			admin.createTable(descriptor);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据表名删除表
	 * 
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		try {
			Admin admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				// 首先disable
				admin.disableTable(TableName.valueOf(tableName));
				// drop
				admin.deleteTable(TableName.valueOf(tableName));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据tableName获取table对象
	 * 
	 * @param tableName
	 * @return
	 */
	public Table getTable(String tableName) {
		try {
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据计数器计算行健
	 * 
	 * @param tableActivity
	 * @param familyActivityActivity
	 * @param qualifierGidActivityId
	 * @return
	 */
	public Long getId(String tableName, String family, String qualifier) {
		Table table = getTable(tableName);
		try {
			return table.incrementColumnValue(Bytes.toBytes(ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID),
					Bytes.toBytes(family), Bytes.toBytes(qualifier), 1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0L;
	}

	/**
	 * rowKey为Integer,value为Integer
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family, String qualifier, Integer value) {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * rowKey为Integer,value为Double
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family, String qualifier, Double value) {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * rowKey为Integer,value为String
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family, String qualifier, String value) {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * rowKey为String,value为String
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, String rowKey, String family, String qualifier, String value) {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * rowKey为String,value为Integer
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, String rowKey, String family, String qualifier, Integer value) {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
