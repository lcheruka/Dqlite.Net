using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Dqlite.Net
{
    public interface IDqliteService : IHostedService
    {
        Task OnRoleChangeAsync(bool isLeader, CancellationToken cancellationToken);
    }
}