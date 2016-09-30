package com.sf.stinift.resource.hive;

import org.junit.Test;


/**
 * Created by scut_DELL on 15/11/30.
 */
public class HiveConnectionTest {

    @Test
    public void testGetThriftClient() throws Exception {
        HiveConnection connection = new HiveConnection(
                "192.168.10.42", 10000, "192.168.10.42", 9083, "default", "yudaer", "");
        connection.open();
        HiveSession hiveSession = connection.openSession();
        System.out.println(hiveSession.getTableLocation("default", "test"));

        hiveSession.close();
        connection.close();


//        TTransport tTransport_ms = new TSocket("10.211.55.14", 9083);
//        TProtocol protocol_ms = new TBinaryProtocol(tTransport_ms);
//        tTransport_ms.open();
//        ThriftHiveMetastore.Client metastoreClient = (new ThriftHiveMetastore.Client.Factory()).getClient(protocol_ms);
//        Table table  = metastoreClient.get_table("default", "test");
//        System.out.println(table.getSd().getLocation());
    }

}
