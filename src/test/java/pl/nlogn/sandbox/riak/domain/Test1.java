package pl.nlogn.sandbox.riak.domain;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.RiakIndex;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.RiakVClock;

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
* Date: 12/7/12
* Time: 9:54 PM
*/
public class Test1 {
    @RiakKey
    private String key;

    @RiakVClock
    private VClock vclock;

    private String value;

    @RiakIndex(name = "index1_bin")
    private String value_index1;

    private long timestamp;

    // required by Jackson for JSON serialization
    public Test1() {}

    public String getKey() {
        return key;
    }

    public VClock getVclock() {
        return vclock;
    }

    public void setVclock(VClock vclock) {
        this.vclock = vclock;
    }

    public Test1(String key, String value) {
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Test1{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", vclock=" + vclock.asString() +
                '}';
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue_index1() {
        return value_index1;
    }

    public void setValue_index1(String value_index1) {
        this.value_index1 = value_index1;
    }
}
