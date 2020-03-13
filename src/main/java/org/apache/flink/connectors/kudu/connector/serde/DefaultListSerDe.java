/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.connector.serde;

import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.kudu.Schema;

import java.util.List;

public class DefaultListSerDe implements KuduListSerialization<List<KuduRow>>, KuduListDeserialization<List<KuduRow>> {

    @Override
    public List<KuduRow> deserialize(List<KuduRow> row) {
        return row;
    }

    @Override
    public List<KuduRow> serialize(List<KuduRow> value) {
        return value;
    }

    @Override
    public DefaultListSerDe withSchema(Schema schema) {
        return this;
    }

}
