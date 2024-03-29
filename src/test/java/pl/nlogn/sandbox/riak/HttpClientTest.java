package pl.nlogn.sandbox.riak;


import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.query.indexes.BinIndex;
import org.junit.Assert;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMScripts;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import pl.nlogn.sandbox.riak.domain.Test1;
import pl.nlogn.sandbox.riak.domain.Test1Mutation;

import java.util.List;

/*
* Copyright 2012 Nlogn Paweł Sidoryk
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
* User: pawel
* Date: 11/28/12
* Time: 9:41 PM
*/
@RunWith(BMUnitRunner.class)
public class HttpClientTest {
    //public static final String[] RIAK_URL = {"http://192.168.0.31:8098/riak", "http://192.168.0.32:8098/riak", "http://192.168.0.33:8098/riak"};
    //public static final String[] RIAK_URL = {"http://192.168.0.4:8098/riak"};
    public static final String[] RIAK_URL = {"http://127.0.0.1:10018/riak","http://127.0.0.1:10028/riak","http://127.0.0.1:10038/riak","http://127.0.0.1:10048/riak"};

    public static final String REC_BUCKET1 = "testclient1";

    public static final String REC_KEY1 = "conc1";

    public static final String REC_KEY2 = "conc2";

    public static final String REC_VALUE1 = "value1";

    public static final String REC_VALUE2 = "value2";
    public static final String REC_VALUE2_INDEX1 = "value2_index1";

    private IRiakClient httpClient;

    private Bucket myBucket;

    @Before
    public void setUp() throws Exception {
        httpClient = RiakFactory.httpClient(RIAK_URL[0]);
        myBucket = httpClient.fetchBucket(REC_BUCKET1).execute();
        myBucket = httpClient.updateBucket(myBucket).allowSiblings(true).execute();
        myBucket.store(REC_KEY1, REC_VALUE1).execute();
        Test1 pojo = new Test1(REC_KEY2, REC_VALUE2);
        pojo.setValue_index1(REC_VALUE2_INDEX1);
        myBucket.store(pojo).execute();
    }

    @After
    public void tearDown() throws Exception {
        myBucket.delete(REC_KEY1).execute();
        myBucket.delete(REC_KEY2).execute();
        httpClient.shutdown();
    }

    @Test
    public void testListKeys() throws Exception {
        Bucket testBucket = httpClient.fetchBucket(REC_BUCKET1).execute();
        int keyCnt = 0;
        for (String key: testBucket.keys()) {
            ++ keyCnt;
        }
        Assert.assertEquals(2, keyCnt);
    }

    @Test
    public void testFetch() throws Exception {
        FreshDeleteConflictResolver res1 = new FreshDeleteConflictResolver();
        IRiakObject myObject = myBucket.fetch(REC_KEY1).withResolver(res1).execute();
        System.out.println(myObject.getValueAsString());
        System.out.println("vclock: " + myObject.getVClockAsString());

        myObject.setValue(myObject.getValueAsString() + 1);
        myObject = myBucket.store(myObject).returnBody(true).withResolver(res1).execute();
        System.out.println(myObject.getValueAsString());
        System.out.println("vclock: " + myObject.getVClockAsString());

        myBucket.delete(REC_KEY1).execute();
        myObject = myBucket.store(REC_KEY1, REC_VALUE1).returnBody(true).withResolver(res1).execute();
        System.out.println(myObject.getValueAsString());
        System.out.println("vclock: " + myObject.getVClockAsString());
        Assert.assertEquals(2, res1.getSiblingsCount());
    }

    @Test
    public void testFetchPojo() throws Exception {
        FreshPojoDeleteConflictResolver res1 = new FreshPojoDeleteConflictResolver();
        Test1 myObject = myBucket.fetch(REC_KEY2, Test1.class).withResolver(res1).execute();
        System.out.println(myObject);

        Mutation<Test1> mutation = new Test1Mutation(null, "1");
        myObject = myBucket.store(myObject).returnBody(true).withMutator(mutation).withResolver(res1).execute();
        System.out.println(myObject);

        myBucket.delete(REC_KEY2).execute();
        myObject = myBucket.fetch(REC_KEY2, Test1.class).withResolver(res1).execute();
        Assert.assertNull(myObject);

        myObject = new Test1(REC_KEY2, REC_VALUE2);
        mutation = new Test1Mutation(myObject, "1");

        myObject = myBucket.store(myObject).withMutator(mutation).returnBody(true).withResolver(res1).execute();

        System.out.println(myObject);
        Assert.assertEquals(1, res1.getSiblingsCount());
    }

    @Test
    public void testDifferentClientIdsPojo() throws Exception {
        Test1 obj1 = myBucket.fetch(REC_KEY2, Test1.class).execute();

        myBucket.store(obj1).execute();

        try {
            Test1 obj2 = myBucket.store(obj1).withoutFetch().returnBody(true).execute();
            Assert.fail("An exception should be thrown here");
        } catch (UnresolvedConflictException e) {
        }
    }

    @Test
    public void testFetchPojoWithIndex() throws Exception {
        List<String> keys = myBucket.fetchIndex(BinIndex.named("index1_bin")).withValue(REC_VALUE2_INDEX1).execute();
        System.out.println(keys);
        Assert.assertEquals(1, keys.size());
    }

    @Test
    @BMScript(value="testUpdate_parallel1", dir="target/test-classes")
    public void testUpdate_Parallel1() throws Exception {
        int clientCount = 50;
        HttpSingleRecordChangeCommand[] updates = new HttpSingleRecordChangeCommand[clientCount];
        DataChangeExecutor[] executors = new DataChangeExecutor[clientCount];
        for (int i = 0; i < clientCount; ++ i) {
            if (i % 2 == 0) {
                updates[i] = new UpdateChangeCommand(RIAK_URL[(i/2) % 2], REC_BUCKET1, REC_KEY1);
            } else {
                updates[i] = new DeleteChangeCommand(RIAK_URL[(i/2) % 2], REC_BUCKET1, REC_KEY1);
            }
            executors[i] = new DataChangeExecutor("Change" + i, updates[i]);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < clientCount; ++ i) {
            executors[i].start();
        }
        for (int i = 0; i < clientCount; ++ i) {
            executors[i].join();
        }
        System.out.println("siblings: pre : deletedPre : post : deletedPost");
        for (int i = 0; i < clientCount; ++ i) {
            System.out.print(updates[i].getSiblingsCountPre() + " : " + updates[i].getDeletedSiblingsCountPre()
                    + " : " + updates[i].getSiblingsCountPost() + " : " + updates[i].getDeletedSiblingsCountPost());
            // NOTE the resolution of "lastModified" is very low, it looks like the resolution is 1 second
            System.out.println(" : " + (updates[i].getRiakObject() != null ? (updates[i].getRiakObject().getLastModified().getTime() - start) + " : " + updates[i].getRiakObject().getValueAsString() : ""));
        }
    }

    @Test
    @BMScripts(
            scripts={@BMScript(value="testUpdate_parallel1", dir="target/test-classes")
            ,@BMScript(value="testStoreObject", dir="target/test-classes")}
    )
    public void testPojoUpdate_Parallel1() throws Exception {
        int clientCount = 50;
        HttpSingleRecordPojoChangeCommand[] updates = new HttpSingleRecordPojoChangeCommand[clientCount];
        DataChangeExecutor[] executors = new DataChangeExecutor[clientCount];
        for (int i = 0; i < clientCount; ++ i) {
            if (i % 2 == 0) {
                updates[i] = new PojoUpdateChangeCommand(RIAK_URL[(i/2) % 2], REC_BUCKET1, REC_KEY2);
            } else {
                updates[i] = new PojoDeleteChangeCommand(RIAK_URL[(i/2) % 2], REC_BUCKET1, REC_KEY2);
            }
            executors[i] = new DataChangeExecutor("Change" + i, updates[i]);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < clientCount; ++ i) {
            executors[i].start();
        }
        for (int i = 0; i < clientCount; ++ i) {
            executors[i].join();
        }
        System.out.println("siblings: pre : deletedPre : post : deletedPost");
        for (int i = 0; i < clientCount; ++ i) {
            System.out.print(updates[i].getSiblingsCountPre() + " : " + updates[i].getDeletedSiblingsCountPre()
                    + " : " + updates[i].getSiblingsCountPost() + " : " + updates[i].getDeletedSiblingsCountPost());
            System.out.println(" : " + (updates[i].getRiakObject() != null ? (updates[i].getRiakObject().getTimestamp() - start) + " : " + updates[i].getRiakObject().getValue() : ""));
        }
    }

    private class UpdateChangeCommand extends HttpSingleRecordChangeCommand {

        public UpdateChangeCommand(String clientUrl, String bucket, String key) {
            super(clientUrl, bucket, key, new FreshDeleteConflictResolver(), new FreshDeleteConflictResolver());
        }

        @Override
        public void execute() throws Exception {
            IRiakObject myObject = myBucket.fetch(getKey()).withResolver(resPre).execute();
            if (myObject != null) {
                myObject.setValue(myObject.getValueAsString() + 1);
                setRiakObject(myBucket.store(myObject).withoutFetch().returnBody(true).withResolver(resPost).execute());
            } else {
                setRiakObject(myBucket.store(getKey(), REC_VALUE1).returnBody(true).withResolver(resPost).execute());
            }
            httpClient.shutdown();
        }
    }

    private class DeleteChangeCommand extends HttpSingleRecordChangeCommand {

        public DeleteChangeCommand(String clientUrl, String bucket, String key) {
            super(clientUrl, bucket, key, new FreshDeleteConflictResolver(), new FreshDeleteConflictResolver());
        }

        @Override
        public void execute() throws Exception {
            IRiakObject myObject = myBucket.fetch(getKey()).withResolver(resPre).execute();
            if (myObject != null) {
                myBucket.delete(myObject).fetchBeforeDelete(false).execute();
            }
            httpClient.shutdown();
        }
    }

    private class PojoUpdateChangeCommand extends HttpSingleRecordPojoChangeCommand {

        public PojoUpdateChangeCommand(String clientUrl, String bucket, String key) {
            super(clientUrl, bucket, key, new FreshPojoDeleteConflictResolver(), new FreshPojoDeleteConflictResolver());
        }

        @Override
        public void execute() throws Exception {
            Test1 myObject = null;
            TombstoneSkippingRetrier retrier = new TombstoneSkippingRetrier();
            try {
                myObject = myBucket.fetch(getKey(), Test1.class).withRetrier(retrier).withResolver(resPre).execute();
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " Bug 3: ");
            }
            if (myObject != null) {
                Mutation<Test1> mutation = new Test1Mutation(myObject, "1");
                try {
                    setRiakObject(myBucket.store(myObject).withoutFetch().withMutator(mutation).withRetrier(retrier).returnBody(true).withResolver(resPost).execute());
                } catch (Exception e) {
                    System.out.println(Thread.currentThread().getName() + " Bug 2: ");
                }
            } else {
                myObject = new Test1(getKey(), REC_VALUE2);
                Mutation<Test1> mutation = new Test1Mutation(myObject, "1");
                try {
                    setRiakObject(myBucket.store(myObject).withMutator(mutation).withRetrier(retrier).returnBody(true).withResolver(resPost).execute());
                } catch (Exception e) {
                    System.out.println(Thread.currentThread().getName() + " Bug 4: ");
                }
            }
            httpClient.shutdown();
        }
    }

    private class PojoDeleteChangeCommand extends HttpSingleRecordPojoChangeCommand {

        public PojoDeleteChangeCommand(String clientUrl, String bucket, String key) {
            super(clientUrl, bucket, key, new FreshPojoDeleteConflictResolver(), new FreshPojoDeleteConflictResolver());
        }

        @Override
        public void execute() throws Exception {
            Test1 myObject = null;
            TombstoneSkippingRetrier retrier = new TombstoneSkippingRetrier();
            try {
                myObject = myBucket.fetch(getKey(), Test1.class).withRetrier(retrier).withResolver(resPre).execute();
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " Bug 5: ");
            }
            if (myObject != null) {
                myBucket.delete(myObject).fetchBeforeDelete(false).execute();
            }
            httpClient.shutdown();
        }
    }
}
