using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dqlite.Net
{
    internal static class TaskExtensions
    {
        internal static async Task<T> WithCancellation<T>(this Task<T> task, TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken))
        {
            var tcs = new TaskCompletionSource<bool>();
            using(var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            using(cts.Token.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true), tcs))
            {
                if(timeout != default(TimeSpan))
                {
                    cts.CancelAfter(timeout);
                }
                
                if(task != await Task.WhenAny(task, tcs.Task))
                {
                    throw new TaskCanceledException(task);
                }
            }
            return await task;
        }

        internal static async Task WithCancellation(this Task task, TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken))
        {
            var tcs = new TaskCompletionSource<bool>();
            using(var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            using(cts.Token.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true), tcs))
            {
                if(timeout != default(TimeSpan))
                {
                    cts.CancelAfter(timeout);
                }
                
                if(task != await Task.WhenAny(task, tcs.Task))
                {
                    throw new TaskCanceledException(task);
                }
            }
            await task;
        }
    }
}