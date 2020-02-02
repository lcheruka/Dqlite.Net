using System.Threading;
using System.Threading.Tasks;

namespace Dqlite.Net
{
    public abstract class DqliteLeaderService : IDqliteService
    {
        private CancellationTokenSource cts;
        private Task executeTask;

        public virtual Task StartAsync(CancellationToken cancellationToken)
            => Task.CompletedTask;

        public virtual Task StopAsync(CancellationToken cancellationToken)
            => OnRoleChangeAsync(false, cancellationToken);

        protected abstract Task ExecuteAsync(CancellationToken cancellationToken);

        public virtual async Task OnRoleChangeAsync(bool isLeader, CancellationToken cancellationToken)
        {
            if(isLeader && this.cts == null)
            {
                try
                {
                    this.cts = new CancellationTokenSource();
                    this.executeTask = ExecuteAsync(this.cts.Token);
                }
                catch
                {
                    this.cts = null;
                    throw;
                }
            }
            else if(!isLeader && this.cts != null && !this.cts.IsCancellationRequested)
            {
                try
                {
                    this.cts.Cancel();
                    await Task.WhenAny(this.executeTask, Task.Delay(-1, cancellationToken)).ConfigureAwait(false);
                }
                finally
                {
                    this.cts = null;
                }
            }
        }
    }
}