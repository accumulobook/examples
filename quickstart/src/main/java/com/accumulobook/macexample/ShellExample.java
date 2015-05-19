package com.accumulobook.macexample;

import com.google.common.io.Files;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.core.util.shell.Shell;

import java.io.File;
import java.lang.Runnable;

public class ShellExample implements Runnable {

  @Override
  public void run() {
    File tempDir = null;
    MiniAccumuloCluster mac = null;

    try {
      tempDir = Files.createTempDir();
      tempDir.deleteOnExit();

      final String PASSWORD = "pass1234";

      mac = new MiniAccumuloCluster(tempDir, PASSWORD);
      System.out.println("Starting the MiniAccumuloCluster in " + tempDir.getAbsolutePath());
      System.out.println("Zookeeper is " + mac.getZooKeepers());
      System.out.println("Instance is " + mac.getInstanceName());
      mac.start();

      String[] args = new String[] {"-u", "root", "-p", PASSWORD, "-z",
        mac.getInstanceName(), mac.getZooKeepers()};

      Shell.main(args);

    } catch (Exception e) {
      System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
      System.exit(1);
    } finally {
        if (null != tempDir) {
            tempDir.delete();
        }
        if (null != mac) {
            try {
                mac.stop();
            } catch (Exception e) {
                System.err.println("Error stopping MiniAccumuloCluster: " + e.getMessage());
                System.exit(1);
            }
        }
    }
  }

  public static void main(String[] args) {
    System.out.println("\n   ---- Initializing Accumulo Shell\n");

    ShellExample shell = new ShellExample();
    shell.run();

    System.exit(0);
  }
}
