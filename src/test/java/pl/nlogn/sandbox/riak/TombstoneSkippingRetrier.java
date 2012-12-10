package pl.nlogn.sandbox.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.raw.RiakResponse;

import java.util.concurrent.Callable;

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
* Date: 12/9/12
* Time: 5:04 PM
*/
public class TombstoneSkippingRetrier extends DefaultRetrier {
    public static final int ATTEMPTS_DEFAULT = 3;

    public TombstoneSkippingRetrier() {
        super(ATTEMPTS_DEFAULT);
    }

    @Override
    public <T> T attempt(Callable<T> command) throws RiakRetryFailedException {
        T t = super.attempt(command);
        if (t instanceof RiakResponse) {
            RiakResponse response = (RiakResponse) t;
            IRiakObject[] objects = response.getRiakObjects();
            int tombstoneCnt = 0;
            int nonTombstoneCnt = 0;
            for (IRiakObject object: objects) {
                if (object.getValue().length == 0) {
                    ++ tombstoneCnt;
                } else {
                    ++ nonTombstoneCnt;
                }
            }

            if (tombstoneCnt == 0) {
                return t;
            }

            IRiakObject[] objectsReplacement = new IRiakObject[nonTombstoneCnt];
            nonTombstoneCnt = 0;
            for (IRiakObject object: objects) {
                if (object.getValue().length > 0) {
                    objectsReplacement[nonTombstoneCnt ++] = object;
                }
            }

            RiakResponse responseReplacent = new RiakResponse(response.getVclockBytes(), objectsReplacement);
            return (T) responseReplacent;
        }
        return t;
    }
}
