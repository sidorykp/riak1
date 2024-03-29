package pl.nlogn.sandbox.riak.byteman;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.RiakResponse;

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
* Date: 12/8/12
* Time: 10:45 PM
*/
public class RiakResponseHelper {
    public void showRiakResponse(RiakResponse response) {
        IRiakObject[] objects = response.getRiakObjects();
        int tombstoneCnt = 0;
        for (IRiakObject object: objects) {
            if (object.getValue().length == 0) {
                ++ tombstoneCnt;
            }
        }
        if (tombstoneCnt > 0) {
            System.out.println(Thread.currentThread().getName() + " tombstones: " + tombstoneCnt);
        }
    }
}
