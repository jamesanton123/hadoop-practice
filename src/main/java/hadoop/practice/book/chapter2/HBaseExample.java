package hadoop.practice.book.chapter2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Joiner;

public class HBaseExample {

	/**
	 * Make sure region server is started, and the other hbase services are up
	 * and running. Also check to make sure zookeeper service is running. You
	 * can do this by logging into ambari as admin/admin and starting them from
	 * there.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		new HBaseExample().beginExample();

	}

	private void beginExample() {
		try {
			Configuration conf = getConfigurationHBase();
			Connection conn = ConnectionFactory.createConnection(conf);
			Admin hba = conn.getAdmin();

			System.out.println("Cluster Status: " + hba.getClusterStatus().toString());
			System.out.println("Namespace descriptors: " + Arrays.asList(hba.listNamespaceDescriptors()));
			System.out.println("Table names: " + Arrays.asList(hba.listTableNames()));

			// Add a new table
			final String tName = "myLittleHBaseTable";
			final TableName tableName = TableName.valueOf(tName);

			// Delete the table if it exists
			if (hba.isTableAvailable(tableName)) {
				hba.disableTable(tableName);
				hba.deleteTable(tableName);
			}

			// Create the table
			final HTableDescriptor hbdesc = new HTableDescriptor(tableName);
			HColumnDescriptor keyDescriptor = new HColumnDescriptor("Row Key");
			HColumnDescriptor nameDescriptor = new HColumnDescriptor("Name");
			HColumnDescriptor jobDataDescriptor = new HColumnDescriptor("Job Data");
			hbdesc.addFamily(keyDescriptor);
			hbdesc.addFamily(nameDescriptor);
			hbdesc.addFamily(jobDataDescriptor);
			hba.createTable(hbdesc);

			System.out.println("Table was available? : " + hba.isTableAvailable(tableName));
			System.out.println("Table names: " + Arrays.asList(hba.listTableNames()));

			// Add a row to the table
			HTable h = new HTable(conf, tableName);
			Put p = new Put(Bytes.toBytes("1"));
			p.addColumn(Bytes.toBytes("Name"), Bytes.toBytes("First"), Bytes.toBytes("James"));
			p.addColumn(Bytes.toBytes("Name"), Bytes.toBytes("Last"), Bytes.toBytes("Anton"));
			p.addColumn(Bytes.toBytes("Job Data"), Bytes.toBytes("Income"), Bytes.toBytes("$1 per hour"));
			p.addColumn(Bytes.toBytes("Job Data"), Bytes.toBytes("Years working"), Bytes.toBytes("3"));
			h.put(p);
			h.flushCommits();
			h.close();

			System.out.println("Added row to table");

			// Perform a get against the table
			h = new HTable(conf, tableName);
			Get get = new Get(Bytes.toBytes("1"));
			get.addFamily(Bytes.toBytes("Job Data"));
			// Optionally specify columns to get
			get.addColumn(Bytes.toBytes("Job Data"), Bytes.toBytes("Income"));
			get.setMaxVersions(3);
			Result result = h.get(get);
			System.out.println(result);
			h.close();

			// Using the scanner to get data from the table
			h = new HTable(conf, tableName);
			Scan scan = new Scan(Bytes.toBytes("1"));
			scan.addColumn(Bytes.toBytes("Job Data"), Bytes.toBytes("Income"));
			scan.addColumn(Bytes.toBytes("Name"), Bytes.toBytes("Last"));
			scan.setFilter(new PageFilter(25));
			ResultScanner scanner = h.getScanner(scan);
			for (Result r : scanner) {
				System.out.println("Scanner found row: " + r);
				System.out.println("   Last Name = " + Bytes.toString(r.getValue(Bytes.toBytes("Name"), Bytes.toBytes("Last"))));
			}
			h.close();
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	private Configuration getConfigurationHBase() {
		System.setProperty("hadoop.home.dir", "/");

		Configuration configuration = HBaseConfiguration.create();

		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "hortonworks.hbase.vm");
		configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

		return configuration;
	}
}