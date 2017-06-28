/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.sysprocs.NibbleDeletes;

public class TestNibbleDeletes extends RegressionSuite {

    public void testBasic() throws IOException, ProcCallException, InterruptedException {
        System.out.println("STARTING basic.....");

        Client client = this.getClient();
        for (int i = 0; i < 100; i++) {
            client.callProcedure("P1.insert", i, i);
        }
        VoltTable vt;
        vt = client.callProcedure("@NibbleDeletes", "delete from p1 where points > 0;").getResults()[0];
        System.out.println(vt);

        vt = client.callProcedure("@AdHoc", "select count(*) from P1;").getResults()[0];
        System.out.println(vt);

        vt = client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
        System.out.println(vt);

        client.drain();
    }

    public void testCancelNibbleDelete() throws IOException, ProcCallException, InterruptedException {
        System.out.println("STARTING testCancelNibbleDelete.....");

        final int BATCH_SIZE = NibbleDeletes.BATCH_DELETE_TUPLE_COUNT;
        // to make the deletes slow
        NibbleDeletes.BATCH_DELETE_TUPLE_COUNT = 1;

        try {
            Client client = this.getClient();
            ProcedureCallback NullCallBack = new ProcedureCallback() {
                @Override
                public void clientCallback(ClientResponse clientResponse) throws Exception {
                    assertEquals(ClientResponse.SUCCESS, clientResponse.getStatus());
                }
            };
            for (int i = 0; i < 10000; i++) {
                client.callProcedure(NullCallBack, "P1.insert", i, i);
            }
            client.drain();
            VoltTable vt;

            ProcedureCallback callback = new ProcedureCallback() {
                @Override
                public void clientCallback(ClientResponse clientResponse) throws Exception {
                    assertEquals(ClientResponse.GRACEFUL_FAILURE, clientResponse.getStatus());
                    assertEquals("@NibbleDeletes is cancelled", clientResponse.getStatusString());
                }
            };

            client.callProcedure(callback,
                    "@NibbleDeletes", "delete from p1 where points > 0;");

            Thread.sleep(100);
            ClientResponse cr = client.callProcedure("@CancelNTProcedure", "@NibbleDeletes");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            String expectedResult = "Successfully informed 1 NT-Procedure @NibbleDeletes";
            assertTrue(cr.getStatusString(),
                    cr.getStatusString().contains(expectedResult));
            assertTrue(cr.getResults()[0].toString(), cr.getResults()[0].toString().contains(expectedResult));

            vt = client.callProcedure("@AdHoc", "select count(*) from P1;").getResults()[0];
            System.err.println("count(*): " + vt.asScalarLong());
            assertTrue(vt.asScalarLong() < 10000);

            client.drain();
        } finally {
            NibbleDeletes.BATCH_DELETE_TUPLE_COUNT = BATCH_SIZE;
        }
    }

    public void testWindowExample() throws IOException, ProcCallException, InterruptedException {
        System.out.println("STARTING testWindowExample.....");

        Client client = this.getClient();

        VoltTable vt = client.callProcedure("@NibbleDeletes", "DELETE FROM timedata WHERE update_ts <= NOW ").getResults()[0];

        System.err.println(vt);

        vt = client.callProcedure("@CancelNTProcedure", "@NibbleDeletes").getResults()[0];

        System.err.println(vt);

    }



    public TestNibbleDeletes(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestNibbleDeletes.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE p1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "POINTS INTEGER, " +
                "PRIMARY KEY (ID) );" +
                "Partition table p1 on column ID;" +

                "CREATE TABLE timedata (" +
                "uuid VARCHAR(36) NOT NULL," +
                "val BIGINT NOT NULL," +
                "update_ts TIMESTAMP NOT NULL," +
                "CONSTRAINT PK_timedate PRIMARY KEY (uuid, update_ts));" +
                "PARTITION TABLE timedata ON COLUMN uuid;"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

        config = new LocalCluster("nibble-deletes-onehost.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        //      config = new LocalCluster("nibble-deletes-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        //      success = config.compile(project);
        //      assertTrue(success);
        //      builder.addServerConfig(config);

        return builder;
    }

}