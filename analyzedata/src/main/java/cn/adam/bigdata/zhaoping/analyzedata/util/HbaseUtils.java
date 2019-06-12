package cn.adam.bigdata.zhaoping.analyzedata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtils {
    private static HbaseUtils hbase = null;
    private Admin admin;
    private Connection connection;

    private HbaseUtils() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "slave1");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean tableExists(String tn) throws IOException {
        return admin.tableExists(TableName.valueOf(tn));
    }

    public void tableCreate(String tn, boolean delexists, String... cfs) throws IOException {
        if (tableExists(tn) && delexists) {
            tableDelete(tn);
        }

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tn));

        for (String string : cfs) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(string);
            descriptor.addFamily(columnDescriptor);
        }

        admin.createTable(descriptor);
    }

    public void tableDelete(String tn) throws IOException {
        TableName name = TableName.valueOf(tn);
        if (!tableExists(tn)) {
            return;
        }
        admin.disableTable(name);
        admin.deleteTable(name);
    }


    public void put(String tn, String rowKey, String cf, String cn, long ts, byte[] value) throws IOException {
        if (!tableExists(tn)) {
            return;
        }
        byte[] rkb = rowKey == null?null: Bytes.toBytes(rowKey);
        byte[] cfb = cf == null?null:Bytes.toBytes(cf);
        byte[] cnb = cn == null?null:Bytes.toBytes(cn);
        byte[] vb = value;

        Table table = connection.getTable(TableName.valueOf(tn));

        Put put = new Put(rkb);
        if (ts == -1)
            put.addColumn(cfb, cnb, vb);
        else
            put.addColumn(cfb, cnb, ts, vb);

        table.put(put);
    }

    public void delete(String tn, String rowKey) throws IOException {
        delete(tn, rowKey, null, null, -1, false);
    }
    public void delete(String tn, String rowKey, String cf, String cn) throws IOException {
        delete(tn, rowKey, cf, cn, -1, false);
    }
    public void delete(String tn, String rowKey, String cf, String cn, long ts) throws IOException {
        delete(tn, rowKey, cf, cn, ts, false);
    }
    public void deletes(String tn, String rowKey, String cf, String cn) throws IOException {
        delete(tn, rowKey, cf, cn, -1, true);
    }
    public void deletes(String tn, String rowKey, String cf, String cn, long ts) throws IOException {
        delete(tn, rowKey, cf, cn, ts, true);
    }
    private void delete(String tn, String rowKey, String cf, String cn, long ts, boolean s) throws IOException {
        if (!tableExists(tn)) {
            return;
        }

        byte[] rkb = rowKey == null?null:Bytes.toBytes(rowKey);
        byte[] cfb = cf == null?null:Bytes.toBytes(cf);
        byte[] cnb = cn == null?null:Bytes.toBytes(cn);

        Table table = connection.getTable(TableName.valueOf(tn));
        Delete delete = new Delete(rkb);

        if (cfb != null) {
            if (s) {
                if (ts == -1)
                    delete.addColumns(cfb, cnb);
                else
                    delete.addColumns(cfb, cnb, ts);
            }else {
                if (ts == -1)
                    delete.addColumn(cfb, cnb);
                else
                    delete.addColumn(cfb, cnb, ts);
            }
        }

        table.delete(delete);
    }

    public Result get(String tn, String rowKey) throws IOException {
        return get(tn, rowKey, null, null);
    }
    public Result get(String tn, String rowKey, String cf, String cn) throws IOException {
        if (!tableExists(tn)) {
            return null;
        }
        byte[] rkb = rowKey == null?null:Bytes.toBytes(rowKey);
        byte[] cfb = cf == null?null:Bytes.toBytes(cf);
        byte[] cnb = cn == null?null:Bytes.toBytes(cn);

        Table table = connection.getTable(TableName.valueOf(tn));
        Get get = new Get(rkb);
        if (cfb != null) {
            get.addColumn(cfb, cnb);
        }

        return table.get(get);
    }


    public ResultScanner tableScan(String tn, Filter filter) throws IOException {
        if (!tableExists(tn)) {
            return null;
        }

        Table table = connection.getTable(TableName.valueOf(tn));
        Scan scan = new Scan();
        if (filter!=null)
            scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public void close() {
        hbase = null;
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static HbaseUtils getInstance() {
        if (hbase == null)
            hbase = new HbaseUtils();

        return hbase;
    }



    public void put(String tn, String rowKey, String cf, String cn, int value) throws IOException {
        put(tn, rowKey, cf, cn, -1, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, int value) throws IOException {
        put(tn, rowKey, cf, cn, ts, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long value) throws IOException {
        put(tn, rowKey, cf, cn, -1, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, long value) throws IOException {
        put(tn, rowKey, cf, cn, ts, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, double value) throws IOException {
        put(tn, rowKey, cf, cn, -1, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, double value) throws IOException {
        put(tn, rowKey, cf, cn, ts, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, float value) throws IOException {
        put(tn, rowKey, cf, cn, -1, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, float value) throws IOException {
        put(tn, rowKey, cf, cn, ts, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, boolean value) throws IOException {
        put(tn, rowKey, cf, cn, -1, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, boolean value) throws IOException {
        put(tn, rowKey, cf, cn, ts, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, short value) throws IOException {
        put(tn, rowKey, cf, cn, -1, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, short value) throws IOException {
        put(tn, rowKey, cf, cn, ts, Bytes.toBytes(value));
    }
    public void put(String tn, String rowKey, String cf, String cn, String value) throws IOException {
        byte[] s = value==null?null:Bytes.toBytes(value);
        put(tn, rowKey, cf, cn, -1, s);
    }
    public void put(String tn, String rowKey, String cf, String cn, long ts, String value) throws IOException {
        byte[] s = value==null?null:Bytes.toBytes(value);
        put(tn, rowKey, cf, cn, ts, s);
    }
}
