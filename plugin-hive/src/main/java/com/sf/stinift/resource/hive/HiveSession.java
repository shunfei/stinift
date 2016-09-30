package com.sf.stinift.resource.hive;

import com.sf.stinift.exchange.Bee;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.log.Logger;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetColumnsResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.hive.service.cli.thrift.TStatusCode;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by scut_DELL on 15/11/30.
 */
public class HiveSession {

    private static Logger logger = new Logger(HiveSession.class);
    private TCLIService.Client client;
    private ThriftHiveMetastore.Client metastoreClient;
    private TSessionHandle tSessionHandle;
    private TProtocolVersion protocol;

    public HiveSession(TCLIService.Client client, ThriftHiveMetastore.Client metastoreClient,
                       TSessionHandle tSessionHandle, TProtocolVersion protocol) {
        this.client = client;
        this.tSessionHandle = tSessionHandle;
        this.metastoreClient = metastoreClient;
        this.protocol = protocol;
    }

    public Fetchable execute(String query) throws TException, RuntimeException {
        TExecuteStatementReq tExecuteStatementReq = new TExecuteStatementReq(tSessionHandle, query);
        TExecuteStatementResp executeStatementResp = client.ExecuteStatement(tExecuteStatementReq);
        if (executeStatementResp.getStatus().getStatusCode() != TStatusCode.SUCCESS_STATUS) {
            throw new RuntimeException("execute query:" + query + ". error!");
        }
        TOperationHandle tOperationHandle = executeStatementResp.getOperationHandle();
        return queryResult(tOperationHandle);
    }

    private Fetchable queryResult(final TOperationHandle tOperationHandle) {
        return new Fetchable() {
            @Override
            public Bee fetch() {
                List<Bee> bees = fetch(1);
                if (bees != null && bees.size() != 0) {
                    return bees.get(0);
                } else {
                    return null;
                }
            }

            @Override
            public List<Bee> fetch(int number) {
                try {
                    TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(tOperationHandle, TFetchOrientation.FETCH_NEXT, number);
                    TFetchResultsResp tFetchResultsResp = client.FetchResults(tFetchResultsReq);
                    TRowSet tRowSet = tFetchResultsResp.getResults();

                    RowSet fetchedRows = RowSetFactory.create(tRowSet, protocol);

                    List<Bee> bees = new ArrayList<Bee>();
                    Iterator<Object[]> iterator = fetchedRows.iterator();
                    while (iterator.hasNext()) {
                        Object[] columns = iterator.next();
                        Row row = new Row(fetchedRows.numColumns());
                        for (int j = 0; j < fetchedRows.numColumns(); j++) {
                            if (columns[j] == null) {
                                row.setField(j, "");
                            } else {
                                row.setField(j, columns[j].toString());
                            }
                        }
                        bees.add(row);
                    }

                    return bees;
                } catch (TException e) {
                    logger.error(e.getCause(), e.getMessage());
                    return null;
                }
            }
        };
    }

    public String[] getColumns(String table) throws TException {
        TGetColumnsReq tGetColumnsReq = new TGetColumnsReq();
        tGetColumnsReq.setSessionHandle(tSessionHandle);
        tGetColumnsReq.setTableName(table);
        TGetColumnsResp tGetColumnsResp = client.GetColumns(tGetColumnsReq);

        Fetchable fetchable = queryResult(tGetColumnsResp.getOperationHandle());
        List<String> result = new ArrayList<String>();
        Row row;
        while ((row = (Row) fetchable.fetch()) != null) {
            result.add(row.getField(3));
        }

        return result.toArray(new String[0]);
    }

    public String getTableLocation(String db, String table) throws TException {
        Table table1 = metastoreClient.get_table(db, table);
        return table1.getSd().getLocation();
    }

    public void close() {
        try {
            TCloseSessionReq tCloseSessionReq = new TCloseSessionReq();
            tCloseSessionReq.setSessionHandle(tSessionHandle);
            client.CloseSession(tCloseSessionReq);
        } catch (TException e) {
            logger.error(e.getCause(), e.getMessage());
        }
    }

}
