using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Dqlite.Net
{
    internal static class NativeMethods
    {
        public static void CheckError(int code)
        {
            if (code != 0)
            {
                throw new InvalidOperationException();
            }
        }
        public delegate int dqlite_node_connect_func(IntPtr handler, string address, int fd);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern int dqlite_node_create(ulong id, string address, string dataDir, out IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern void dqlite_node_destroy(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern int dqlite_node_set_bind_address(IntPtr node, string address);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern string dqlite_node_get_bind_address(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern int dqlite_node_set_network_latency(IntPtr node, ulong nanoseconds);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern int dqlite_node_set_connect_func(IntPtr node, dqlite_node_connect_func func, IntPtr arg);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern int dqlite_node_start(IntPtr node);

        [DllImport("dqlite", CallingConvention = CallingConvention.Cdecl)]
        public static extern int dqlite_node_stop(IntPtr node);
    }
}
