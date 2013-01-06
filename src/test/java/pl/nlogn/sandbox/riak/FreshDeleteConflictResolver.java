package pl.nlogn.sandbox.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.ConflictResolver;

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
* Date: 12/2/12
* Time: 9:15 PM
*/
public class FreshDeleteConflictResolver implements ConflictResolver<IRiakObject>, AuditableConflictResolver {
    private int siblingsCount;

    private int deletedSiblingsCount;

    @Override
    public IRiakObject resolve(Collection<IRiakObject> siblings) {
        siblingsCount = siblings.size();
        deletedSiblingsCount = 0;
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
            } else {
                deletedSiblingsCount ++;
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
