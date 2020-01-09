using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dqlite.Net
{
    internal sealed partial class DqliteConnector : IDisposable, IAsyncDisposable
    {
        internal delegate T ResponseParser<T>(ResponseTypes type, int size, Memory<byte> data);
        private const ulong VERSION = 1;
        private const byte REVISION = 0;

        internal DqliteConnectionStringBuilder Settings { get; }
        private readonly bool connectLeader;       

        private Stream stream;

        public DqliteConnector(bool connectLeader = false)
            : this(new DqliteConnectionStringBuilder(), connectLeader)
        {
        }

        public DqliteConnector(DqliteConnectionStringBuilder settings, bool connectLeader)
        {
            this.Settings = settings;
            this.connectLeader = connectLeader;
        }

        private void SetSocketOptions(Socket socket)
        {
            if (socket.AddressFamily == AddressFamily.InterNetwork)
            {
                socket.NoDelay = true;
            }
                
            if (Settings.SocketReceiveBufferSize > 0)
            {
                socket.ReceiveBufferSize = Settings.SocketReceiveBufferSize;
            }
                
            if (Settings.SocketSendBufferSize > 0)
            {
                socket.SendBufferSize = Settings.SocketSendBufferSize;
            }

            if (Settings.TcpKeepAlive)
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            }
        }
        
        
        public void Dispose()
        {
            ((IDisposable)stream).Dispose();
        }

        public ValueTask DisposeAsync()
        {
            return ((IAsyncDisposable)stream).DisposeAsync();
        }
    }
}