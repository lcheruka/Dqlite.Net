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
        private delegate T RetryFunc<T>(ReadOnlySpan<byte> data);
        private delegate Task<T> RetryFuncAsync<T>(ReadOnlyMemory<byte> data);
        
        private const ulong VERSION = 1;
        private const byte REVISION = 0;

        internal DqliteConnectionStringBuilder Settings { get; }
        private readonly bool connectLeader;
        private readonly int attempts;
        private readonly int delay;

        private Stream stream;

        public DqliteConnector(bool connectLeader = false)
            : this(new DqliteConnectionStringBuilder(), connectLeader)
        {
        }

        public DqliteConnector(DqliteConnectionStringBuilder settings, bool connectLeader)
        {
            this.Settings = settings;
            this.connectLeader = connectLeader;
            this.attempts = 1000;
            this.delay = 250;
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

        private T Retry<T>(RetryFunc<T> func, ReadOnlySpan<byte> data, CancellationToken cancellationToken = default)
        {
            for(int i = 0; i < this.attempts; ++i)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return func(data);
                }
                catch(DqliteException ex) when(ex.ErrorCode == 5)
                {
                    cancellationToken.WaitHandle.WaitOne(this.delay);
                }
            }

            throw new DqliteException(5, "database is locked");
        }

        private async Task<T> RetryAsync<T>(RetryFuncAsync<T> func, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            for(int i = 0; i < this.attempts; ++i)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return await func(data);
                }
                catch(DqliteException ex) when(!cancellationToken.IsCancellationRequested && ex.ErrorCode == 5)
                {
                    await Task.Delay(this.delay);
                }
            }

            throw new DqliteException(5, "database is locked");
        }
        
        public void Dispose()
        {
            try
            {
                ((IDisposable)stream)?.Dispose();
            }
            catch
            {
                // ignored
            }
            this.stream = null;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await (((IAsyncDisposable)stream)?.DisposeAsync() ?? new ValueTask());
            }
            catch
            {
                // ignored
            }
            this.stream = null;
        }
    }
}