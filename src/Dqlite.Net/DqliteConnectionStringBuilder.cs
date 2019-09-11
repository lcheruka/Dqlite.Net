using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Globalization;

namespace Dqlite.Net
{
    public class DqliteConnectionStringBuilder : DbConnectionStringBuilder
    {
        private const int DefaultPort = 6543;
        private const string DatabaseKeyword = "Database";
        private const string LeaderOnlyKeyword = "LeaderOnly";
        private const string ServersKeyword = "Servers";

        private enum Keywords
        {
            Database,
            Servers,
            LeaderOnly
        }

        private static readonly IReadOnlyList<string> _validKeywords;
        private static readonly IReadOnlyDictionary<string, Keywords> _keywords;

        private string _database = string.Empty;
        private string[] _servers =new string[]{$"127.0.0.1:{DefaultPort}"};
        private bool _leaderOnly = false;

        static DqliteConnectionStringBuilder()
        {
            var validKeywords = new string[6];
            validKeywords[(int)Keywords.Database] = DatabaseKeyword;
            validKeywords[(int)Keywords.Servers] = ServersKeyword;
            validKeywords[(int)Keywords.LeaderOnly] = LeaderOnlyKeyword;
            _validKeywords = validKeywords;

            _keywords = new Dictionary<string, Keywords>(8, StringComparer.OrdinalIgnoreCase)
            {
                [DatabaseKeyword] = Keywords.Database,
                [ServersKeyword] = Keywords.Servers,
                [LeaderOnlyKeyword] = Keywords.LeaderOnly
            };
        }

        public DqliteConnectionStringBuilder()
        {
        }

        public DqliteConnectionStringBuilder(string connectionString)
            => ConnectionString = connectionString;


        public virtual string Database
        {
            get => _database;
            set => base[DatabaseKeyword] = _database = value;
        }

        public virtual string[] Servers
        {
            get => _servers;
            set
            {
                _servers = value;
                base[ServersKeyword] = string.Join(",",_servers);
            }
        }

        public virtual bool LeaderOnly
        {
            get => _leaderOnly;
            set => base[LeaderOnlyKeyword] = _leaderOnly = value;
        }

        public override ICollection Keys
            => new ReadOnlyCollection<string>((string[])_validKeywords);

        /// <summary>
        ///     Gets a collection containing the values used by the connection string.
        /// </summary>
        /// <value>A collection containing the values used by the connection string.</value>
        public override ICollection Values
        {
            get
            {
                var values = new object[_validKeywords.Count];
                for (var i = 0; i < _validKeywords.Count; i++)
                {
                    values[i] = GetAt((Keywords)i);
                }

                return new ReadOnlyCollection<object>(values);
            }
        }
        
        public override object this[string keyword]
        {
            get => GetAt(GetIndex(keyword));
            set
            {
                if (value == null)
                {
                    Remove(keyword);

                    return;
                }

                switch (GetIndex(keyword))
                {
                    case Keywords.Database:
                        this.Database = Convert.ToString(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.Servers:
                        var serverListString =  Convert.ToString(value, CultureInfo.InvariantCulture);
                        var servers =  serverListString.Split(',', StringSplitOptions.RemoveEmptyEntries);
                        for(int i = 0; i < servers.Length; ++i)
                        {
                            var server = servers[i];
                            if(server.IndexOf(':') == -1)
                            {
                                server += $":{DefaultPort}";
                            }

                            if(!Utils.TryParseAddress(server, out _, out _))
                            {
                                throw new FormatException();
                            }

                            servers[i] = server;
                        }

                        this.Servers = servers;
                        return;
                    case Keywords.LeaderOnly:
                        this.LeaderOnly = Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                        return;
                    default:
                        return;
                }
            }
        }

        public override void Clear()
        {
            base.Clear();

            for (var i = 0; i < _validKeywords.Count; i++)
            {
                Reset((Keywords)i);
            }
        }

        public override bool ContainsKey(string keyword)
            => _keywords.ContainsKey(keyword);

        public override bool Remove(string keyword)
        {
            if (!_keywords.TryGetValue(keyword, out var index)
                || !base.Remove(_validKeywords[(int)index]))
            {
                return false;
            }

            Reset(index);

            return true;
        }

        public override bool ShouldSerialize(string keyword)
            => _keywords.TryGetValue(keyword, out var index) && base.ShouldSerialize(_validKeywords[(int)index]);

        public override bool TryGetValue(string keyword, out object value)
        {
            if (!_keywords.TryGetValue(keyword, out var index))
            {
                value = null;

                return false;
            }

            value = GetAt(index);

            return true;
        }

        private object GetAt(Keywords index)
        {
            switch (index)
            {
                case Keywords.Database:
                    return Database;
                case Keywords.Servers:
                    return Servers;
                case Keywords.LeaderOnly:
                    return LeaderOnly;
                default:
                    return null;
            }
        }

        private static Keywords GetIndex(string keyword)
            => !_keywords.TryGetValue(keyword, out var index)
                ? throw new ArgumentException(nameof(keyword))
                : index;

        private void Reset(Keywords index)
        {
            switch (index)
            {
                case Keywords.Database:
                    _database = string.Empty;
                    return;
                case Keywords.Servers:
                    _servers = new string[]{$"127.0.0.1:{DefaultPort}"};
                    return;
                case Keywords.LeaderOnly:
                    _leaderOnly = false;
                    return;
                default:
                    return;
            }
        }
    }
}
