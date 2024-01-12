package org.apache.spark.client

import org.apache.spark.rpc.RpcEndpointRef

case class RegisterWorker(workerId: String, workerMemory: Int, workerCores: Int) {
  var lastHeartBeatTime: Long = _

}
