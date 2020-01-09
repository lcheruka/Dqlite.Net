using System;

namespace  Dqlite.Net
{
    public class DqliteNodeOptions
    {
        public string Address {get; set;}
        public DqliteNodeConnect DialFunction {get; set;}
        public TimeSpan NetworkLatency {get;set;}

        public DqliteNodeOptions WithDialFunction(DqliteNodeConnect dial)
        {
            this.DialFunction = dial;
            return this;
        }

        public DqliteNodeOptions WithBindAddress(string address)
        {
            this.Address = address;
            return this;
        }

        public DqliteNodeOptions WithNetworkLatency(TimeSpan latency)
        {
            this.NetworkLatency = NetworkLatency;
            return this;
        }
    }
}