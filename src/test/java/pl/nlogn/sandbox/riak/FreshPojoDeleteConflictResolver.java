package pl.nlogn.sandbox.riak;

import com.basho.riak.client.cap.ConflictResolver;
import pl.nlogn.sandbox.riak.domain.Test1;

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
* Date: 12/8/12
* Time: 3:29 PM
*/
public class FreshPojoDeleteConflictResolver implements ConflictResolver<Test1>, AuditableConflictResolver {
    private int siblingsCount;

    private int deletedSiblingsCount;

    @Override
    public Test1 resolve(Collection<Test1> siblings) {
        siblingsCount = siblings.size();
        Test1 ret = null;
        long lastModifiedMax = 0;
        for (Test1 s: siblings) {
            if (ret == null) {
                ret = s;
                lastModifiedMax = s.getTimestamp();
            } else if (s.getTimestamp() > lastModifiedMax) {
                ret = s;
                lastModifiedMax = s.getTimestamp();
            }
        }
        return ret;
    }

    public int getSiblingsCount() {
        return siblingsCount;
    }

    public int getDeletedSiblingsCount() {
        return deletedSiblingsCount;
    }
}
