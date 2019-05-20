package org.apache.spark.network

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{DeleteMethod, GetMethod}
import org.apache.spark.internal.Logging

/***
  * 请求 spiltsize
  * 可以换成redis 或者 zk
  *
  */
object RestHttpClient   {


  def removeOnePartition(stage : Int,index : Int) = {
    val client = new HttpClient
    // 设置代理服务器地址和端口
    val method = new DeleteMethod("http://RestHttpServer:7999/spark/get/remove/"+stage+"/"+index)
    client.executeMethod(method)
    method.releaseConnection()
  }

  def removeall() ={
    val client = new HttpClient
    // 设置代理服务器地址和端口
    val method = new DeleteMethod("http://RestHttpServer:7999/spark/get/removeall")
    client.executeMethod(method)
    method.releaseConnection()
  }

  def get(stage:String,index:String) : String = {
    val client = new HttpClient
    // 设置代理服务器地址和端口
    val method = new GetMethod("http://RestHttpServer:7999/spark/get/"+stage+"/"+index)
    client.executeMethod(method)
    val result = method.getResponseBodyAsString
    method.releaseConnection()

    result
  }

  def post(stage:String,index:String,size : String) = {
    val client = new HttpClient
    val method = new GetMethod("http://RestHttpServer:7999/spark/post/"+stage+"/"+index+"/"+size)
    client.executeMethod(method)
    val result = method.getResponseBodyAsString
    method.releaseConnection()
  }

}
