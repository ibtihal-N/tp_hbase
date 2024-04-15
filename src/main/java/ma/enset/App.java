package ma.enset;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class App {
    public static final String TABLE_NAME="users";
    public static final String CF_PERSONAL_DATA="personal_data";
    public static final String CF_PROFESIONAL_DATA="profesional_data";
    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zookeeper");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master","hbase-master:16000");

        try{
            Connection connection=ConnectionFactory.createConnection(conf);
            Admin admin=connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder=TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESIONAL_DATA));
            TableDescriptor tableDescriptor=builder.build();
            if(!admin.tableExists(tableName)){
                admin.createTable(tableDescriptor);
                System.out.println("La table est bien créée");
            }
            else{
                System.out.println("La table est déja créée");
            }
            Table table=connection.getTable(tableName);
            Put put=new Put(Bytes.toBytes("1111"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("name"),Bytes.toBytes("Karim Ahmed"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("age"),Bytes.toBytes("50"));
            put.addColumn(Bytes.toBytes(CF_PROFESIONAL_DATA),Bytes.toBytes("diplome"),Bytes.toBytes("ingénieur"));
            table.put(put);
            System.out.println("La ligne a été bien inséréée");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}