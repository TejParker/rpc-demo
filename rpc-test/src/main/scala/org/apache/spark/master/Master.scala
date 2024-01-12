package org.apache.spark.master

import org.apache.spark.{HeartBeat, SecurityManager, SparkConf}
import org.apache.spark.client.RegisterWorker
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

class Master(
              val rpcEnv: RpcEnv,
            ) extends ThreadSafeRpcEndpoint {

  // 实现receive方法，接收异步消息
  override def receive: PartialFunction[Any, Unit] = {
    case "test" => println("Master接收到了测试消息")
    case RegisterWorker(workerId, workerMemory, workerCores) => {
      println(s"Master收到Worker的注册消息：workerId: $workerId, worker memory：$workerMemory, worker cores: $workerCores")
      val workerEndpointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 6666), "Worker")
      workerEndpointRef.send("Registered")
    }
    case HeartBeat(workerId) => {

    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象
    val conf = new SparkConf
    // 创建SecurityManager（安全管理，对系统资源的访问进行检查和限制）
    val securityMgr = new SecurityManager(conf)
    // 创建RpcEnv，并指定名称和ip地址，端口等。
    val rpcEnv = RpcEnv.create("SparkMaster", "localhost", 7777, conf, securityMgr)
    // 创建Master RpcEndPoint
    val master = new Master(rpcEnv)
    // 将Master RpcEndPoint的引用传入到setupEndpoint方法中，并指定名称【用来向RpcEnv注册RpcEndPoint，返回注册的RpcEndPoint的引用】
    val masterEndpointRef = rpcEnv.setupEndpoint("Master", master)
    // 通过masterEndpointRef向MasterEndpoint发送消息
    masterEndpointRef.send("test")
    // 将程序挂起，等待退出
    rpcEnv.awaitTermination()
  }
}