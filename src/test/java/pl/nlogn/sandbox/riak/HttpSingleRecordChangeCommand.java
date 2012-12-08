package pl.nlogn.sandbox.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import pl.nlogn.sandbox.riak.domain.Test1;

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
* Time: 9:11 PM
*/
public abstract class HttpSingleRecordChangeCommand implements DataChangeCommand {
    private IRiakClient httpClient;

    private Bucket myBucket;

    private String key;

    protected FreshDeleteConflictResolver resPre;

    protected FreshDeleteConflictResolver resPost;

    private IRiakObject riakObject;

    public HttpSingleRecordChangeCommand(String clientUrl, String bucket, String key
            , FreshDeleteConflictResolver resPre, FreshDeleteConflictResolver resPost) {
        this.resPre = resPre;
        this.resPost = resPost;
        this.key = key;
        try {
            httpClient = RiakFactory.httpClient(clientUrl);
            myBucket = httpClient.fetchBucket(bucket).execute();
        } catch (RiakException e) {
            e.printStackTrace();
        }
    }

    public String getKey() {
        return key;
    }

    public int getSiblingsCountPre() {
        return resPre.getSiblingsCount();
    }

    public int getSiblingsCountPost() {
        return resPost.getSiblingsCount();
    }

    public int getDeletedSiblingsCountPre() {
        return resPre.getDeletedSiblingsCount();
    }

    public int getDeletedSiblingsCountPost() {
        return resPost.getDeletedSiblingsCount();
    }

    public IRiakObject getRiakObject() {
        return riakObject;
    }

    public void setRiakObject(IRiakObject riakObject) {
        this.riakObject = riakObject;
    }
}
