using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dqlite.Net
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            
            if(args.Length != 3){
                Console.Error.WriteLine("Incorrect number of arguments");
                Environment.Exit(-1);
            }

            var id = ulong.Parse(args[0]);
            var address = args[1];
            var dataDir = args[2];

            if(!Directory.Exists(dataDir))
            {
                Directory.CreateDirectory(dataDir);
            }

            var tcs = new TaskCompletionSource<bool>();
            Console.CancelKeyPress += (sender, args2) =>
            {
                args2.Cancel = true;
                tcs.TrySetResult(true);
            };

            using (var node = DqliteNode.Create(id, address, dataDir))
            {
                node.Start();
                await tcs.Task;
            }            
        }
    }
}
