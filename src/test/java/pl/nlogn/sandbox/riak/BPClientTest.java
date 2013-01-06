package pl.nlogn.sandbox.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import org.junit.Test;

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
* Date: 1/4/13
* Time: 10:59 PM
*/
public class BPClientTest {
    public static final String RIAK_IP = "127.0.0.1";
    public static final int RIAK_PORT = 10017;
    //public static final String RIAK_IP = "192.168.0.4";
    //public static final int RIAK_PORT = 8098;

    @Test
    public void testListKeys() throws Exception {
        IRiakClient myPbClient = RiakFactory.pbcClient(RIAK_IP, RIAK_PORT);
        Bucket testBucket = myPbClient.fetchBucket("training").execute();

        int keyCnt = 0;
        for (String key: testBucket.keys()) {
            System.out.println("Key: " + key);
            IRiakObject obj = testBucket.fetch(key).execute();
            ++ keyCnt;
        }
        System.out.println("Key count: " + keyCnt);
    }
}
