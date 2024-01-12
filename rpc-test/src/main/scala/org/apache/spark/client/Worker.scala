package org.apache.spark.client

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

class Worker(
            val rpcEnv: RpcEnv
            ) extends ThreadSafeRpcEndpoint {

  var masterEndpoint: RpcEndpointRef = _

  override def onStart(): Unit = {
    // 向Master发送建立连接请求
    masterEndpoint = rpcEnv.setupEndpointRef(RpcAddress("localhost", 7777), "Master")
    // 向Master发送注册Worker请求
    masterEndpoint.send(RegisterWorker("worker-01", 10240, 8))


  }

  override def receive: PartialFunction[Any, Unit] = {
    case "Registered" => println("worker成功注册！")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case "response" => println(888)
  }

}

object Worker {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val securityMgr = new SecurityManager(conf)

    val rpcEnv: RpcEnv = RpcEnv.create("SparkWorker", "localhost", 6666, conf, securityMgr)

    rpcEnv.setupEndpoint("Worker", new Worker(rpcEnv))

    rpcEnv.awaitTermination()
  }
}
