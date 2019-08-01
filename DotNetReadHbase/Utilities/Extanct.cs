using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace System
{
    /// <summary>
    /// Extanct 的摘要说明
    /// </summary>
    public static class Extanct
    {
        /// <summary>
        /// 获取字节数组的Utf-8字符串
        /// </summary>
        /// <returns></returns>
        public static string ToUTF8String(this byte[] bytes ) {

            return System.Text.UTF8Encoding.UTF8.GetString(bytes);
        }
        /// <summary>
        /// 返回字符串的ＵＴＦ－８字节数组
        /// </summary>
        /// <param name=""></param>
        /// <returns></returns>

        public static byte[] ToUTF8Bytes(this string str )
        {

            return System.Text.UTF8Encoding.UTF8.GetBytes(str);
        }
    }
}