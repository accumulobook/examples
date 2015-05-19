package com.accumulobook.macexample;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class Example {

    private String instanceName;
    private String zookeepers;
    private String rootPassword;

    private String TABLE = "example1";

    public Example(String instanceName, String zookeepers, String rootPassword) {
        this.instanceName = instanceName;
        this.zookeepers = zookeepers;
        this.rootPassword = rootPassword;
    }

    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        Connector conn = instance.getConnector("root", new PasswordToken(rootPassword));
        return conn;
    }

    public void ensureRootAuths(List<String> auths) throws AccumuloException, AccumuloSecurityException {
        Connector connector = getConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations((String[]) auths.toArray()));
    }

    public void ensureTableCreated() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        Connector conn = getConnector();
        if (!conn.tableOperations().exists(TABLE)) {
            conn.tableOperations().create(TABLE);
        };
    }

    public void insertRow(String rowid, String cf, String cq, String val, String auths) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Connector conn = getConnector();
        BatchWriter bw = conn.createBatchWriter(TABLE, new BatchWriterConfig());

        Mutation m = new Mutation(new Text(rowid));
        if (null == auths) {
            m.put(new Text(cf), new Text(cq), new Value(val.getBytes()));
        } else {
            m.put(new Text(cf), new Text(cq), new ColumnVisibility(auths), new Value(val.getBytes()));
        }
        bw.addMutation(m);

        bw.close();
    }

    public void insertRow(String rowid, String cf, String cq, String val) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        insertRow(rowid, cf, cq, val, null);
    }

    public HashMap<Key,Value> scanRow(String rowid, List<String> auths) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        HashMap<Key, Value> result = new HashMap<Key, Value>();
        Connector conn = getConnector();
        Authorizations scanAuths = new Authorizations();
        if (null != auths) {
            scanAuths = new Authorizations((String[]) auths.toArray());
        }
        Scanner scanner = conn.createScanner(TABLE, scanAuths);
        scanner.setRange(new Range(rowid));
        for (Entry<Key,Value> entry : scanner) {
          result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public HashMap<Key, Value> scanRow(String rowid) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        return scanRow(rowid, null);
    }
}
