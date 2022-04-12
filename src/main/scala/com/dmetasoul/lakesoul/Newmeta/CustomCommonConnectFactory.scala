/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.Newmeta

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ConstantReconnectionPolicy, DowngradingConsistencyRetryPolicy, RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnectorConf}

object CustomCommonConnectFactory extends CassandraConnectionFactory {
  protected def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val builder = Cluster.builder()
    val poolingOpts = new PoolingOptions
    poolingOpts.setMaxRequestsPerConnection(HostDistance.LOCAL, 600)
    poolingOpts.setMaxRequestsPerConnection(HostDistance.REMOTE, 600)
    poolingOpts.setMaxConnectionsPerHost(HostDistance.LOCAL, 600)
    poolingOpts.setMaxConnectionsPerHost(HostDistance.REMOTE, 600)
    poolingOpts.setIdleTimeoutSeconds(600)
    poolingOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, 20)
    poolingOpts.setCoreConnectionsPerHost(HostDistance.REMOTE, 20)
    poolingOpts.setHeartbeatIntervalSeconds(60)
    poolingOpts.setPoolTimeoutMillis(1000 * 60)


    builder.addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      .withPoolingOptions(poolingOpts)
      .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
      .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
      .withSocketOptions(new SocketOptions()
        .setTcpNoDelay(true)
        .setConnectTimeoutMillis(MetaCommon.META_CONNECT_TIMEOUT)
        .setReadTimeoutMillis(MetaCommon.META_READ_TIMEOUT))
      .withCredentials(MetaCommon.META_USERNAME, MetaCommon.META_PASSWORD)
      .withCompression(ProtocolOptions.Compression.LZ4)
      .withQueryOptions(new QueryOptions()
        .setConsistencyLevel(ConsistencyLevel.ALL)
        .setSerialConsistencyLevel(ConsistencyLevel.SERIAL))
      .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
  }

  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    clusterBuilder(conf).build()
  }
}

