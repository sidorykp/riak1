package pl.nlogn.sandbox.riak.domain;

import com.basho.riak.client.cap.Mutation;

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

* Time: 11:09 PM
*/
public class Test1Mutation implements Mutation<Test1> {
    private String sufix;

    private Test1 defaultValue;

    public Test1Mutation(Test1 defaultValue, String sufix) {
        this.defaultValue = defaultValue;
        this.sufix = sufix;
    }

    @Override
    public Test1 apply(Test1 original) {
        if (original != null) {
            original.setValue(original.getValue() + sufix);
            original.setTimestamp(System.currentTimeMillis());
        } else {
            return defaultValue;
        }
        return original;
    }
}
