using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using HbaseHelper;
using LoggerHelper;

namespace UnitTestProject
{
    [TestClass]
    public class UnitTest1
    {
        public static List<string> listTempRowkey = new List<string>();
        [TestMethod]
        public void TestMethod1()
        {
            
            int blockingCapacity = Convert.ToInt32(AppSettingInfo.appHbaseDownloadDataCapacity);
            ReadRowKey(AppSettingInfo.appSettingReadPath);
            if (listTempRowkey.Count > blockingCapacity)
            {
                LoggerManager.Create().InfoWrite(String.Format("Hbase查询下载数据超过最大容量{0}条，开始分区下载", blockingCapacity));
                var loop = listTempRowkey.Count / blockingCapacity;
                for (var i = 0; i < loop + 1; i++)
                {
                    var list = i == loop ? listTempRowkey.Skip(i * blockingCapacity).Take(listTempRowkey.Count - i * blockingCapacity).ToList() : listTempRowkey.Skip(i * blockingCapacity).Take(blockingCapacity).ToList();
                }
                Console.Write("ok");
                Console.ReadKey();
            }
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
    }
}
