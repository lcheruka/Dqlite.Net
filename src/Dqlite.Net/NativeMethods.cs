using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Dqlite.Net
{
    public delegate int DqliteNodeConnect(IntPtr handler, string address, ref int fd);
    internal static class NativeMethods
    {
        public static void CheckError(int code, IntPtr node)
        {
            if (code != 0)
            {
                var errorMessgae = dqlite_node_errmsg(node);
                throw new DqliteException((ulong)code, errorMessgae ?? "Unknow error occured");
            }
        }

        [DllImport("sqlite3", CallingConvention = CallingConvention.StdCall)]
        public static extern int sqlite3_config(int value);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern int dqlite_node_create(ulong id, string address, string dataDir, out IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern void dqlite_node_destroy(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern int dqlite_node_set_bind_address(IntPtr node, string address);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern string dqlite_node_get_bind_address(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern int dqlite_node_set_network_latency(IntPtr node, ulong nanoseconds);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern int dqlite_node_set_connect_func(IntPtr node, DqliteNodeConnect func, IntPtr arg);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern int dqlite_node_start(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern int dqlite_node_stop(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.StdCall)]
        public static extern string dqlite_node_errmsg(IntPtr node);
    }
}
