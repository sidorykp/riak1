package pl.nlogn.sandbox.riak;


import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.ConflictResolver;
import junit.framework.Assert;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Date;

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
    public static final String[] RIAK_URL = {"http://192.168.0.4:8098/riak", "http://192.168.0.11:8098/riak", "http://192.168.0.12:8098/riak"};

    public static final String REC_BUCKET1 = "test1";

    public static final String REC_KEY1 = "conc1";

    public static final String REC_VALUE1 = "value1";

    private IRiakClient httpClient;

    private Bucket myBucket;

    @Before
    public void setUp() throws Exception {
        httpClient = RiakFactory.httpClient(RIAK_URL[0]);
        myBucket = httpClient.fetchBucket(REC_BUCKET1).execute();
        myBucket.store(REC_KEY1, REC_VALUE1).execute();
    }

    @After
    public void tearDown() throws Exception {
        myBucket.delete(REC_KEY1).execute();
        httpClient.shutdown();
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
    @BMScript(value="testUpdate_parallel1", dir="target/test-classes")
    public void testUpdate_Parallel1() throws Exception {
        int clientCount = 50;
        UpdateChangeCommand[] updates = new UpdateChangeCommand[clientCount];
        DataChangeExecutor[] executors = new DataChangeExecutor[clientCount];
        for (int i = 0; i < clientCount; ++ i) {
            updates[i] = new UpdateChangeCommand(RIAK_URL[i % 2], REC_BUCKET1, REC_KEY1);
            executors[i] = new DataChangeExecutor("UpdateChange" + i, updates[i]);
        }
        for (int i = 0; i < clientCount; ++ i) {
            executors[i].start();
        }
        for (int i = 0; i < clientCount; ++ i) {
            executors[i].join();
        }
        for (int i = 0; i < clientCount; ++ i) {
            System.out.print(updates[i].getSiblingsCountPre() + " : " + updates[i].getSiblingsCountPost());
            // NOTE the resolution of "lastModified" is very low, it looks like the resolution is 1 second
            System.out.println(" : " + updates[i].getRiakObject().getLastModified().getTime());
        }
    }

    private class UpdateChangeCommand implements DataChangeCommand {
        private IRiakClient httpClient;

        private Bucket myBucket;

        private FreshDeleteConflictResolver resPre;

        private FreshDeleteConflictResolver resPost;

        private IRiakObject riakObject;

        public UpdateChangeCommand(String clientUrl, String bucket, String key) {
            try {
                httpClient = RiakFactory.httpClient(clientUrl);
                myBucket = httpClient.fetchBucket(bucket).execute();
                resPre = new FreshDeleteConflictResolver();
                resPost = new FreshDeleteConflictResolver();
            } catch (RiakException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void execute() throws Exception {
            IRiakObject myObject = myBucket.fetch(REC_KEY1).withResolver(resPre).execute();
            myObject.setValue(myObject.getValueAsString() + 1);
            this.riakObject = myBucket.store(myObject).returnBody(true).withResolver(resPost).execute();
            httpClient.shutdown();
        }

        public int getSiblingsCountPre() {
            return resPre.getSiblingsCount();
        }

        public int getSiblingsCountPost() {
            return resPost.getSiblingsCount();
        }

        public IRiakObject getRiakObject() {
            return riakObject;
        }
    }

    private class FreshDeleteConflictResolver implements ConflictResolver<IRiakObject> {

        private int siblingsCount;

        @Override
        public IRiakObject resolve(Collection<IRiakObject> siblings) {
            siblingsCount = siblings.size();
            IRiakObject ret = null;
            Date lastModifiedMax = null;
            for (IRiakObject s: siblings) {
                if (! "".equals(s.getValueAsString())) {
                    if (ret == null) {
                        ret = s;
                        lastModifiedMax = s.getLastModified();
                    } else if (s.getLastModified().after(lastModifiedMax)) {
                        ret = s;
                        lastModifiedMax = s.getLastModified();
                    }
                }
            }
            return ret;
        }

        public int getSiblingsCount() {
            return siblingsCount;
        }

        public void setSiblingsCount(int siblingsCount) {
            this.siblingsCount = siblingsCount;
        }
    }
}
