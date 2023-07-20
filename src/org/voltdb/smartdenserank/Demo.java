package org.voltdb.smartdenserank;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

public class Demo {

    public static void main(String[] args) {
        try {
            Client c = connectVoltDB("localhost");
            Random r = new Random();

            int count = 1000000;

            int votes = 10000000;

            for (int i = 0; i < votes; i++) {

                double id = r.nextGaussian();

                if (id < 0) {
                    id = id * -1;
                }
                id = id * count;
                id = id % count;

                ComplainOnErrorCallback cb = new ComplainOnErrorCallback();
                c.callProcedure(cb, "ReportRank", 1,  (int) id);

                if (i % 2000 == 0) {

                    for (int z = 0; z < 10; z++) {
                        ClientResponse cr = c.callProcedure("QueryRank", 1, 1, 10, null, 20);
                        // System.out.println(cr.getResults()[0].toFormattedString());
                    }

                    for (int z = 0; z < 10; z++) {
                        ClientResponse cr = c.callProcedure("QueryUserId", 1, (int)id, 3);
                        System.out.println("user_" + ((int) id) + System.lineSeparator() + cr.getResults()[0].toFormattedString());
                    }

                }

                if (i % 5000000 == 3) {
                    ClientResponse cr = c.callProcedure("RemoveOffset", 1);
                    System.out.println(cr.getAppStatusString());
                }
            }

            ClientResponse cr = c.callProcedure("RemoveOffset", 1);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static Client connectVoltDB(String hostname) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            // msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setMaxOutstandingTxns(20000);
            config.setMaxTransactionsPerSecond(200000);
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            client.createConnection(hostname);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    public static void msg(String message) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);
    }
}
