package pl.nlogn.sandbox.riak;


import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.ConflictResolver;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

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
public class HttpClientTest {
    public static final String RIAK_URL = "http://192.168.0.4:8098/riak";

    public static final String REC_KEY1 = "conc1";

    private IRiakClient httpClient;

    private Bucket myBucket;

    @Before
    public void setUp() throws Exception {
        httpClient = RiakFactory.httpClient(RIAK_URL);
        myBucket = httpClient.fetchBucket("test1").execute();
        myBucket.store(REC_KEY1, "foo").execute();
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
        myObject = myBucket.store(REC_KEY1, "foo").returnBody(true).withResolver(res1).execute();
        System.out.println(myObject.getValueAsString());
        System.out.println("vclock: " + myObject.getVClockAsString());
        Assert.assertEquals(2, res1.getSiblingsCount());
    }

    private class FreshDeleteConflictResolver implements ConflictResolver<IRiakObject> {

        private int siblingsCount;

        @Override
        public IRiakObject resolve(Collection<IRiakObject> siblings) {
            siblingsCount = siblings.size();
            IRiakObject ret = null;
            for (IRiakObject s: siblings) {
                if (! "".equals(s.getValueAsString())) {
                    ret = s;
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
