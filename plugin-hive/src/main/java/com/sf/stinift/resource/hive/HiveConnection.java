package com.sf.stinift.resource.hive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.resource.Resource;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnection extends Resource {
    private final String host;
    private final Integer port;
    private final String metaStoreHost;
    private final Integer metaStorePort;
    private final String db;
    private final String user;
    private final String pwd;
    private final String auth;
    private TCLIService.Client client;
    private ThriftHiveMetastore.Client metastoreClient;

    HiveConnection(String host, Integer port, String metaStoreHost,
                   Integer metaStorePort, String db, String user, String pwd, String auth) {
        this.host = host;
        this.port = port;
        this.metaStoreHost = metaStoreHost;
        this.metaStorePort = metaStorePort;
        this.db = db;
        this.pwd = pwd;
        this.user = user;
        this.auth = auth;
    }

    public String getUser() {
        return user;
    }

    public String getDb() {
        return db;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String.format("jdbc:hive2://%s:%d/%s;auth=%s", host, port, db, auth), user, pwd);
    }

    public HiveSession openSession() throws TException {
        HiveSession hiveSession = getHiveSession();
        hiveSession.execute("use " + db);
        return hiveSession;
    }

    @Override
    public void open() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        TTransport tTransport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(tTransport);
        tTransport.open();
        client = (new TCLIService.Client.Factory()).getClient(protocol);

        TTransport tTransport_ms = new TSocket(metaStoreHost, metaStorePort);
        TProtocol protocol_ms = new TBinaryProtocol(tTransport_ms);
        tTransport_ms.open();
        metastoreClient = (new ThriftHiveMetastore.Client.Factory()).getClient(protocol_ms);
    }

    private HiveSession getHiveSession() throws TException {
        TOpenSessionReq tOpenSessionReq = new TOpenSessionReq();
        tOpenSessionReq.setUsername(user);
        tOpenSessionReq.setPassword(pwd);
        TOpenSessionResp tOpenSessionResp = client.OpenSession(tOpenSessionReq);
        return new HiveSession(client, metastoreClient, tOpenSessionResp.getSessionHandle(),
                tOpenSessionResp.getServerProtocolVersion());
    }

    @Override
    public void close() {
        try {
            client.getInputProtocol().getTransport().close();
        } catch (Exception e) {

        }
    }

    @Override
    public String info() {
        return String.format("host: %s, port: %d, db: %s", host, port, db);
    }

    public static class Creator extends Resource.Creator {
        @JsonProperty
        public String host;
        @JsonProperty
        public Integer port;
        @JsonProperty
        public String metaStoreHost;
        @JsonProperty
        public Integer metaStorePort;
        @JsonProperty
        public String db;
        @JsonProperty
        public String user;
        @JsonProperty
        public String pwd;
        @JsonProperty
        public String auth;

        public static int DEFAULT_PORT = 10000;
        public static int DEFAULT_METASTORE_PORT = 9083;

        @SuppressWarnings("unchecked")
        @Override
        public HiveConnection create() {
            if (port == null) {
                port = DEFAULT_PORT;
            }
            if (metaStorePort == null) {
                metaStorePort = DEFAULT_METASTORE_PORT;
            }
            if (metaStoreHost == null) {
                metaStoreHost = host;
            }
            if (auth == null) {
                auth = "NONE";
            }
            return new HiveConnection(host, port, metaStoreHost, metaStorePort, db, user, pwd, auth);
        }
    }
}
