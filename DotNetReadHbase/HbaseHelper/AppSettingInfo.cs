using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HbaseHelper
{
    public class AppSettingInfo
    {
        public static string appSettingIP = ConfigurationManager.AppSettings.Get("IPAddress");
        public static string appSettingPort = ConfigurationManager.AppSettings.Get("Port");
        public static string appSettingReadPath = ConfigurationManager.AppSettings.Get("FileReadPath");
        public static string appSettingWritePath = ConfigurationManager.AppSettings.Get("FileWritePath");
        public static string appSettingTableName = ConfigurationManager.AppSettings.Get("TableName");
        public static string appSettingDeleteRowKeyPath = ConfigurationManager.AppSettings.Get("FileDeleteRowKeyPath");
        public static string appSettingDeleteBackUpPath = ConfigurationManager.AppSettings.Get("FileDeleteBackUpPath");
        public static string appSettingOperation = ConfigurationManager.AppSettings.Get("Operation");
        public static string appHbaseDownloadDataCapacity = ConfigurationManager.AppSettings.Get("HbaseDownloadDataCapacity");
    }
}
