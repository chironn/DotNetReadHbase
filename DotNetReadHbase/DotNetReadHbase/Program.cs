using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using HbaseHelper;
using LoggerHelper;

namespace DotNetReadHbase
{
    class Program
    {
        public static List<string> listTempRowkey;
        public static Dictionary<string, string> dicResult;
        static void Main(string[] args)
        {
            int blockingCapacity = Convert.ToInt32(AppSettingInfo.appHbaseDownloadDataCapacity);
            //LoggerManager.Create().InfoWrite
            Console.Title = "Hbase读取工具";
            LoggerManager.Create().InfoWrite("/*******************************************************************/");
            LoggerManager.Create().InfoWrite(string.Format("服务器地址：{0}", AppSettingInfo.appSettingIP + ":" + AppSettingInfo.appSettingPort));
            LoggerManager.Create().InfoWrite(string.Format("Hbase表名：{0}", AppSettingInfo.appSettingTableName));
            LoggerManager.Create().InfoWrite("是否执行Hbase工具：Y/N");
            var str = Console.ReadLine();
            if (str.ToUpper() != "Y")
            {
                return;
            }
            switch (AppSettingInfo.appSettingOperation)
            {
                case "2":
                    LoggerManager.Create().InfoWrite("模式选择：删除指定RowKey数据并备份至本地文件");
                    LoggerManager.Create().InfoWrite("开始读取本地RowKey...");
                    LoggerManager.Create().InfoWrite(string.Format("查询数据删除指定RowKey文件地址：{0}", AppSettingInfo.appSettingDeleteRowKeyPath));
                    LoggerManager.Create().InfoWrite(string.Format("查询数据删除RowKey备份本地文件地址：{0}", AppSettingInfo.appSettingDeleteBackUpPath));
                    LoggerManager.Create().InfoWrite("本地RowKey读取完成\r\n开始查询Hbase库数据...");
                    ReadRowKey(AppSettingInfo.appSettingDeleteRowKeyPath);
                    break;
                default:
                    LoggerManager.Create().InfoWrite("模式选择：查询数据拷贝至本地文件");
                    LoggerManager.Create().InfoWrite("开始读取本地RowKey...");
                    LoggerManager.Create().InfoWrite(string.Format("读取本地文件地址：{0}", AppSettingInfo.appSettingReadPath));
                    LoggerManager.Create().InfoWrite(string.Format("写入本地文件地址：{0}", AppSettingInfo.appSettingWritePath));
                    ReadRowKey(AppSettingInfo.appSettingReadPath);
                    LoggerManager.Create().InfoWrite("本地RowKey读取完成\r\n开始查询Hbase库数据...");
                    break;
            }
            LoggerManager.Create().InfoWrite("/*******************************************************************/");
            
            try
            {
                if (listTempRowkey.Count > blockingCapacity)
                {
                    LoggerManager.Create().InfoWrite(String.Format("Hbase查询下载数据超过最大容量{0}条，开始分区下载", blockingCapacity));
                    var loop = listTempRowkey.Count / blockingCapacity;
                    for (var i = 0; i < loop+1; i++)
                    {
                        LoggerManager.Create().InfoWrite(string.Format("Hbase库指定数据下载/删除第{0}轮开始,数量{1}...", i + 1, blockingCapacity));
                        dicResult = Helper.ReadKeyHbaseData(i == loop ? listTempRowkey.Skip(i * blockingCapacity).Take(listTempRowkey.Count-i*blockingCapacity).ToList() : listTempRowkey.Skip(i * blockingCapacity).Take(blockingCapacity).ToList()
                            , AppSettingInfo.appSettingIP, Convert.ToInt32(AppSettingInfo.appSettingPort), AppSettingInfo.appSettingTableName);
                        LoggerManager.Create().InfoWrite("Hbase库数据读取完成\r\n开始写本地文件");
                        switch (AppSettingInfo.appSettingOperation)
                        {
                            case "2":
                                WriteHbaseData(AppSettingInfo.appSettingDeleteBackUpPath+i+".txt", dicResult);
                                //LoggerManager.Create().InfoWrite(string.Format("Hbase库指定数据删除第{0}轮开始,数量{1}...",i+1,blockingCapacity));
                                Helper.DeleteHbaseData(i == loop ? listTempRowkey.Skip(i * blockingCapacity).Take(listTempRowkey.Count - i * blockingCapacity).ToList() : listTempRowkey.Skip(i * blockingCapacity).Take(blockingCapacity).ToList()
                                    , AppSettingInfo.appSettingIP,Convert.ToInt32(AppSettingInfo.appSettingPort),AppSettingInfo.appSettingTableName);
                                break;
                            default:
                                //LoggerManager.Create().InfoWrite(string.Format("Hbase库指定数据下载第{0}轮开始,数量{1}...", i+1, blockingCapacity));
                                WriteHbaseData(AppSettingInfo.appSettingWritePath + i + ".txt", dicResult);
                                break;
                        }
                    }
                }
                else
                {
                    dicResult = Helper.ReadKeyHbaseData(listTempRowkey, AppSettingInfo.appSettingIP,
                        Convert.ToInt32(AppSettingInfo.appSettingPort),
                        AppSettingInfo.appSettingTableName);
                    LoggerManager.Create().InfoWrite("Hbase库数据读取完成\r\n开始写本地文件");
                    switch (AppSettingInfo.appSettingOperation)
                    {
                        case "2":
                            WriteHbaseData(AppSettingInfo.appSettingDeleteBackUpPath+".txt", dicResult);
                            LoggerManager.Create().InfoWrite("Hbase库指定数据删除开始...");
                            Helper.DeleteHbaseData(listTempRowkey, AppSettingInfo.appSettingIP,
                                Convert.ToInt32(AppSettingInfo.appSettingPort),
                                AppSettingInfo.appSettingTableName);
                            break;
                        default:
                            WriteHbaseData(AppSettingInfo.appSettingWritePath + ".txt", dicResult);
                            break;
                    }
                }

                LoggerManager.Create().InfoWrite("完成！");
            }
            catch (Exception e)
            {
                LoggerManager.Create().ErrorWrite(string.Format("程序异常：{0}",e.Message));
                throw;
            }
            Console.ReadKey();
        }

        /// <summary>
        /// 获取所需的rowKey
        /// </summary>
        public static void ReadRowKey(string Path)
        {
            listTempRowkey = new List<string>();
            StreamReader sr = new StreamReader(Path, Encoding.Default);
            String line;
            var count = 0;
            while ((line = sr.ReadLine()) != null)
            {
                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }
                listTempRowkey.Add(line.ToString().Trim());
                ++count;
                if (count % 1000 != 0) continue;
                LoggerManager.Create().InfoWrite(string.Format("已加载指定RowKey{0}条", count));
            }
            LoggerManager.Create().InfoWrite(string.Format("共加载指定RowKey{0}条", count));
        }
        public static void WriteHbaseData(string path,Dictionary<string, string> dicResult)
        {
            var fs = new FileStream(path, FileMode.OpenOrCreate);
            var sw = new StreamWriter(fs);
            var count = 0;
            foreach (var item in dicResult)
            {
                sw.WriteLine(item.Key+" " +item.Value);
                ++count;
                if (count % 1000 != 0) continue;
                LoggerManager.Create().InfoWrite(string.Format("已写入指定RowKey数据{0}条", count));
                //LoggerManager.Create().InfoWrite(string.Format("已写入{0}条", ++count));
            }
            //清空缓冲区
            sw.Flush();
            //关闭流
            sw.Close();
            fs.Close();
            LoggerManager.Create().InfoWrite(string.Format("Hbase数据写入成功，共写入数据{0}条", count));
        }
    }
}
