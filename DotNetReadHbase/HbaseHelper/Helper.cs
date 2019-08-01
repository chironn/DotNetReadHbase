using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using LoggerHelper;
using Thrift.Protocol;
using Thrift.Transport;

namespace HbaseHelper
{
    public class Helper
    {
        public static Dictionary<string, string> dicResult = new Dictionary<string, string>();
        public static TTransport transport;
        public Helper(string IPAddress, int Port)
        {
            transport = new TSocket(IPAddress, Port);
        }
        public string TestConnection(string IPAddress, int Port)
        {
            TTransport transport = null;
            try
            {
                //192.168.2.111:60010
                //实例化Socket连接
                transport = new TSocket(IPAddress, Port);
                //实例化一个协议对象
                TProtocol tProtocol = new TBinaryProtocol(transport);
                //实例化一个Hbase的Client对象
                var client = new Hbase.Client(tProtocol);
                //打开连接
                transport.Open();

                return "联接成功..";
            }
            catch (Exception ex)
            {
                return "联接失败.." + ":" + ex.ToString();
            }

            finally
            {
                if (transport != null) { transport.Close(); }
            }
        }

        /// <summary>
        /// 读取Hbase数据
        /// </summary>
        /// <param name="tempList"></param>
        /// <param name="IPAddress"></param>
        /// <param name="Port"></param>
        /// <param name="strTableName"></param>
        /// <returns></returns>
        public static Dictionary<string, string> ReadKeyHbaseData(List<string> tempList, string IPAddress, int Port,
            string strTableName)
        {
            Dictionary<string,string> dis=new Dictionary<string, string>();
            try
            {
                int count = 0;
                if (transport == null)
                {
                    transport = new TSocket(IPAddress, Port);
                }

                //实例化一个协议对象
                TProtocol tProtocol = new TBinaryProtocol(transport);
                //实例化一个Hbase的Client对象
                var client = new Hbase.Client(tProtocol);
                //打开连接
                transport.Open();
                foreach (var temp in tempList)
                {
                    //根据表名，RowKey名来获取结果集
                    var reslut = client.getRow(Encoding.UTF8.GetBytes(strTableName),
                        Encoding.UTF8.GetBytes(temp), null);
                    foreach (var keys in reslut)
                    {
                        foreach (var k in keys.Columns)
                        {
                            if (dis.ContainsKey(Encoding.UTF8.GetString(keys.Row))) continue;
                            dis.Add(Encoding.UTF8.GetString(keys.Row), Encoding.UTF8.GetString(k.Value.Value));
                            ++count;
                            //LoggerManager.Create().InfoWrite(string.Format("已下载{0}条记录", ++count));
                            if (count % 1000 != 0) continue;
                            LoggerManager.Create().InfoWrite(string.Format("已下载指定RowKey{0}条", count));
                        }
                    }
                }
                LoggerManager.Create().InfoWrite(string.Format("Hbases下载指定数据成功，共下载数据{0}条", count));
            }
            catch (Exception ex)
            {
                LoggerManager.Create().ErrorWrite(string.Format("Hbase读取指定数据失败，错误信息：{0}", ex.Message));
            }
            finally
            {
                if (transport != null)
                {
                    transport.Close();
                }
            }
            return dis;
        }

        public static bool DeleteHbaseData(List<string> tempList, string IPAddress, int Port,
            string strTableName)
        {
            Dictionary<string,string> dis=new Dictionary<string, string>();
            try
            {
                int count = 0;
                if (transport == null)
                {
                    transport = new TSocket(IPAddress, Port);
                }

                //实例化一个协议对象
                TProtocol tProtocol = new TBinaryProtocol(transport);
                //实例化一个Hbase的Client对象
                var client = new Hbase.Client(tProtocol);
                //打开连接
                transport.Open();
                byte[] tableName = strTableName.ToUTF8Bytes();
                foreach (var temp in tempList)
                {
                    byte[] row = temp.ToUTF8Bytes();
                    Dictionary<byte[], byte[]> encodedAttributes = new Dictionary<byte[], byte[]>();
                    client.deleteAllRow(tableName, row, encodedAttributes);
                    ++count;
                    if (count % 1000 != 0) continue;
                    LoggerManager.Create().InfoWrite(string.Format("已删除指定RowKey{0}条", count));
                    //LoggerManager.Create().InfoWrite(string.Format("已删除{0}条记录", ++count));

                }
                LoggerManager.Create().InfoWrite(string.Format("Hbases删除指定数据成功，共删除数据{0}条", count));
                return true;
            }
            catch (Exception ex)
            {
                LoggerManager.Create().ErrorWrite(string.Format("Hbases删除指定数据失败，错误信息：{0}",ex.Message));
                return false;
            }
            finally
            {
                if (transport != null)
                {
                    transport.Close();
                }
            }
        }

    }
}
