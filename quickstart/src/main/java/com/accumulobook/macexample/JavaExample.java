package com.accumulobook.macexample;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.Scanner;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

class AbstractCommand {

    protected Connector connection = null;

    public void setConnection(Connector connection) {
        this.connection = connection;
    }
}

class CreateTableCommand extends AbstractCommand {
    @Parameter(names = {"-t","--table"}, description = "Table name to create", required = true)
    private String table;

    public void run() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        System.out.println("Creating table " + table);
        if (connection.tableOperations().exists(table)) {
            throw new RuntimeException("Table " + table + " already exists");
        } else {
            connection.tableOperations().create(table);
            System.out.println("Table created");
        }
    }
}

class InsertRowCommand extends AbstractCommand {
    @Parameter(names = {"-t","--table"}, description = "Table to scan", required = true)
    private String table;

    @Parameter(names = {"-r","--rowid"}, description = "Row Id to insert", required = true)
    private String rowId;

    @Parameter(names = {"-cf","--columnFamily"}, description = "Column Family to insert", required = true)
    private String cf;

    @Parameter(names = {"-cq","--columnQualifier"}, description = "Column Qualifier to insert", required = true)
    private String cq;

    @Parameter(names = {"-val","--value"}, description = "Value to insert", required = true)
    private String val;

    @Parameter(names = {"-a","--auths"}, description = "ColumnVisiblity expression to insert with data")
    private String auths;

    public void run() throws TableNotFoundException, MutationsRejectedException {
        System.out.println("Writing mutation for " + rowId);
        BatchWriter bw = connection.createBatchWriter(table, new BatchWriterConfig());

        Mutation m = new Mutation(new Text(rowId));
        m.put(new Text(cf), new Text(cq), new ColumnVisibility(auths), new Value(val.getBytes()));
        bw.addMutation(m);
        bw.close();
    }
}

class ScanCommand extends AbstractCommand {
    @Parameter(names = {"-t","--table"}, description = "Table to scan", required = true)
    private String table;

    @Parameter(names = {"-r","--row"}, description = "Row to scan")
    private String row;

    @Parameter(names = {"-a","--auths"}, description = "Comma separated list of scan authorizations")
    private String auths;

    private String user;

    public void setUser(String user) {
        this.user = user;
    }

    public void run() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        System.out.println("Scanning " + table);
        Authorizations authorizations = null;
        if ((null != auths) && (!auths.equals("SCAN_ALL"))) {
            System.out.println("Using scan auths " + auths);
            authorizations = new Authorizations(auths.split(","));
        } else {
            System.out.println("Scanning with all user auths");
            authorizations = connection.securityOperations().getUserAuthorizations(user);
        }
        Scanner scanner = connection.createScanner(table, authorizations);
        if ((null != row) && (!row.equals("SCAN_ALL"))) {
            System.out.println("Scanning for row " + row);
            scanner.setRange(new Range(row));
        } else {
            System.out.println("Scanning for all rows");
        }
        System.out.println("Results ->");
        for (Entry<Key,Value> entry : scanner) {
            System.out.println("  " + entry.getKey() + " " + entry.getValue());
        }
    }
}

class GrepCommand extends AbstractCommand {
    @Parameter(names = {"-t","--table"}, description = "Table to scan", required = true)
    private String table;

    @Parameter(names = "--term", description = "Term to grep for in table", required = true)
    private String term;

    private String user;

    public void setUser(String user) {
        this.user = user;
    }

    public void run() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        System.out.println("Grepping " + table);
        Authorizations authorizations = connection.securityOperations().getUserAuthorizations(user);
        Scanner scanner = connection.createScanner(table, authorizations);
        Map<String, String> grepProps = new HashMap<String, String>();
        grepProps.put("term", term);
        IteratorSetting is = new IteratorSetting(25, "sample-grep", GrepIterator.class.getName(), grepProps);
        scanner.addScanIterator(is);
        System.out.println("Results ->");
        for (Entry<Key,Value> entry : scanner) {
            System.out.println("  " + entry.getKey() + " " + entry.getValue());
        }
    }
}

public class JavaExample {

    @Parameter(names = {"-p", "--password"}, description = "Accumulo user password", required = true)
    private String password;

    @Parameter(names = {"-u","--user"}, description = "Accumulo user", required = true)
    private String user;

    @Parameter(names = {"-i","--instance"}, description = "Accumulo instance name", required = true)
    private String instance;

    @Parameter(names = {"-z","--zookeepers"}, description = "Comma-separated list of zookeepers", required = true)
    private String zookeepers;

    public Connector getConnection() throws AccumuloException, AccumuloSecurityException {
        Instance i = new ZooKeeperInstance(instance, zookeepers);
        Connector conn = i.getConnector(user, new PasswordToken(password));
        return conn;
    }

    public static void main(String[] args) {
        JavaExample javaExample = new JavaExample();
        JCommander jc = new JCommander(javaExample);

        CreateTableCommand createTableCommand = new CreateTableCommand();
        jc.addCommand("create", createTableCommand);
        InsertRowCommand insertCommand = new InsertRowCommand();
        jc.addCommand("insert", insertCommand);
        ScanCommand scanCommand = new ScanCommand();
        jc.addCommand("scan", scanCommand);
        GrepCommand grepCommand = new GrepCommand();
        jc.addCommand("grep", grepCommand);

        try {
            jc.parse(args);
            String command = jc.getParsedCommand();
            if (null == command) {
                throw new RuntimeException("You didn't choose a command");
            } else if (command.equals("create")) {
                System.out.println("Running create command");
                createTableCommand.setConnection(javaExample.getConnection());
                createTableCommand.run();
            } else if (command.equals("insert")) {
                System.out.println("Running insert command");
                insertCommand.setConnection(javaExample.getConnection());
                insertCommand.run();
            } else if (command.equals("scan")) {
                System.out.println("Running scan command");
                scanCommand.setConnection(javaExample.getConnection());
                scanCommand.setUser(javaExample.user);
                scanCommand.run();
            } else if (command.equals("grep")) {
                System.out.println("Running grep command");
                grepCommand.setConnection(javaExample.getConnection());
                grepCommand.setUser(javaExample.user);
                grepCommand.run();
            } else {
                throw new RuntimeException("Unrecognized command " + command);
            }
        } catch (Exception e) {
            System.err.println("Error: " +  e.getMessage());
            jc.usage();
        }
    }
}
