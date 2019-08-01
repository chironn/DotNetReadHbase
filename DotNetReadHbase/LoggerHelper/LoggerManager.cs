using log4net;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Reflection;

namespace LoggerHelper
{
    public class LoggerManager
    {
        //String className = new StackTrace().GetFrame(1).GetMethod().ReflectedType.Name; //method.ReflectedType.Name;
        private static ILog _logger = LogManager.GetLogger(new StackTrace().GetFrame(2).GetMethod().ReflectedType.Name);
        public static LoggerManager _instance;
        public static LoggerManager Create()
        {
            return _instance ?? (_instance = new LoggerManager());
            
        }
        public LoggerManager()
        {
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo(AppDomain.CurrentDomain.BaseDirectory + "log4net.config"));
        }
        public void InfoWrite(string message)
        {
            _logger.InfoFormat(message);
        }
        public void ErrorWrite(string message)
        {
            _logger.ErrorFormat(message);
        }
        public void WarnWrite(string message)
        {
            _logger.WarnFormat(message);
        }
    }
}
