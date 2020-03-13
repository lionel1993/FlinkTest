/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.connector.failure;

import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.kudu.client.RowError;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;



/*
  经过测试，此类经常出现异常

  Row error for primary key="e2a20507a3864871aa6ef8ab9be1a7ed\x00\x00\xDE=M\x0EK01", tablet=null, server=null, status=Not found: ([0x00000007DE442100, 0x00000008DE402C80))
  2261 Row error for primary key="1aa58a2577b745ac82efbebae5b4b6f4\x00\x00\xDE>\x13pK01", tablet=null, server=null, status=Not found: ([0x0000000DDE442100, 0x0000000EDE402C80))
  2262 Row error for primary key="f7d1d6f8a86b4b4390efc46dc83784e3\x00\x00\xDE=M\x0EK01", tablet=null, server=null, status=Not found: ([0x00000006DE442100, 0x00000007DE402C80))
  2263 Row error for primary key="44e67ac5e9694bca8e4f70f654f8e92e\x00\x00\xDE=M\x0CK01", tablet=null, server=null, status=Not found: ([0x00000010DE442100, 0x00000011DE402C80))
  2264 Row error for primary key="126394d4905b48e99ff34e2161c9188c\x00\x00\xDE>\x13\xAEK01", tablet=null, server=null, status=Not found: ([0x00000012DE442100, 0x00000013DE402C80))
  2265 Row error for primary key="39bffe588af84017a51cf4436194fc25\x00\x00\xDE=MkK01", tablet=null, server=null, status=Not found: ([0x0000000DDE442100, 0x0000000EDE402C80))

研究后
发现在AUTO_FLUSH_BACKGROUND模式下，插入超过分区范围外的数据，会出现异常
//此异常和使用Update更新没有此主键的数据异常一样，容易混淆

这些key的时间都是超过了kudu的range分区范围，因为日志里有些日志的时间是超过范围的，一般过滤即可

并且不抛出异常，打印异常时间日志
 */

public class DefaultKuduFailureHandler implements KuduFailureHandler {
    @Override
    public void onFailure(List<RowError> failure) throws IOException {
        String errors = failure.stream()
                .map(error -> error.toString() + System.lineSeparator())
                .collect(Collectors.joining());
//        throw new IOException("Error while sending value. \n " + errors);
        System.out.println("Error while sending value. \n " + errors);
    }


    @Override
    public void onFailure(List<RowError> failure, KuduRow row) throws IOException {

        String errors = failure.stream()
                .map(error -> error.toString() + System.lineSeparator())
                .collect(Collectors.joining());
//        throw new IOException("Error while sending value. \n " + row.getTableName() +":"+ row.getOperator() +
//                row.getField("log_id")+","+row.getField("log_time") +","+ row.getField("app_id") +
//                " \n " + errors);

        System.out.println("Error while sending value. \n " + errors);

    }
}
