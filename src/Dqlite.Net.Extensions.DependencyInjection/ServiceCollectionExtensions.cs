using System;
using System.IO;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dqlite.Net
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddDqliteService<TDqliteService>(this IServiceCollection services, Func<IServiceProvider, TDqliteService> implementationFactory) 
            where TDqliteService : class, IDqliteService
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IDqliteService>(implementationFactory));
            return services;
        }

        public static IServiceCollection AddDqliteService<TDqliteService>(this IServiceCollection services) 
            where TDqliteService : class, IDqliteService
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IDqliteService, TDqliteService>());
            return services;
        }

        public static IServiceCollection AddDqlite(this IServiceCollection services, IDqliteNodeStore store, Action<DqliteOptions> configureOptions = null)
        {
            store = store ?? throw new ArgumentNullException(nameof(store));

            var options = new DqliteOptions()
            {
                Id = 1,
                DataDir = Path.Combine(Path.GetTempPath(),"dqlite")
            };
            configureOptions?.Invoke(options);

            if(options.Id == 0)
            {
                throw new ArgumentOutOfRangeException("Id");
            }

            if(string.IsNullOrEmpty(options?.Address))
            {
                throw new ArgumentNullException("Address");
            }

            if(string.IsNullOrEmpty(options?.DataDir))
            {
                throw new ArgumentNullException("DataDir");
            }

            if(options.Id != 1 && store.Get().Any(x => x != options.Address))
            {
                throw new ArgumentException($"{nameof(store)} only contains address for current node");
            }

            services.AddSingleton<IDqliteNodeStore>(store);
            services.AddHostedService<DqliteNodeService>(x => new DqliteNodeService(x.GetServices<IDqliteService>(), store, options));
            return services;
        }
    }
}