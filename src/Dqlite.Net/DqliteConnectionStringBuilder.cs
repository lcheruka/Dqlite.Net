using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Dqlite.Net.Properties;

namespace Dqlite.Net
{
    public class DqliteConnectionStringBuilder : DbConnectionStringBuilder
    {    
        private const string NodesKeyword = "Nodes";
        private const string DataSourceKeyword = "Data Source";
        private const string DataSourceNoSpaceKeyword = "DataSource";
        private const string TcpKeepAliveKeyword = "TcpKeepAlive";
        private const string SocketReceiveBufferSizeKeyword = "SocketReceiveBufferSize";
        private const string SocketSendBufferSizeKeyword = "SocketSendBufferSize";

        private enum Keywords
        {
            Nodes,
            DataSource,
            TcpKeepAliveKeyword,
            SocketReceiveBufferSize,
            SocketSendBufferSize
        }

        private static readonly IReadOnlyList<string> _validKeywords;
        private static readonly IReadOnlyDictionary<string, Keywords> _keywords;

        private string[] _nodes;
        private string _dataSource;
        private bool _tcpKeepAlive;
        private int _socketReceiveBufferSize;
        private int _socketSendBufferSize;

        static DqliteConnectionStringBuilder()
        {
            var validKeywords = new string[5];
            validKeywords[(int)Keywords.Nodes] = NodesKeyword;
            validKeywords[(int)Keywords.DataSource] = DataSourceKeyword;
            validKeywords[(int)Keywords.TcpKeepAliveKeyword] = TcpKeepAliveKeyword;
            validKeywords[(int)Keywords.SocketReceiveBufferSize] = SocketReceiveBufferSizeKeyword;
            validKeywords[(int)Keywords.SocketSendBufferSize] = SocketSendBufferSizeKeyword;
            _validKeywords = validKeywords;

            _keywords = new Dictionary<string, Keywords>(8, StringComparer.OrdinalIgnoreCase)
            {
                [NodesKeyword] = Keywords.Nodes,
                [DataSourceKeyword] = Keywords.DataSource,
                [TcpKeepAliveKeyword] = Keywords.TcpKeepAliveKeyword,
                [SocketReceiveBufferSizeKeyword] = Keywords.SocketReceiveBufferSize,
                [SocketSendBufferSizeKeyword] = Keywords.SocketSendBufferSize,

                // aliases
                [DataSourceNoSpaceKeyword] = Keywords.DataSource
            };
        }

        /// <summary>
        /// The hostname or IP address of the PostgreSQL server to connect to.
        /// </summary>
        public virtual string[] Nodes
        {
            get => _nodes;
            set
            {
                _nodes = value;
                var nodesString = (value != null ? string.Join(",", value) : null);
                base[NodesKeyword] = nodesString;
            }
        }

        /// <summary>
        ///     Gets or sets the database file.
        /// </summary>
        /// <value>The database file.</value>
        public virtual string DataSource
        {
            get => _dataSource;
            set => base[DataSourceKeyword] = (_dataSource = value);
        }

        /// <summary>
        /// Whether to use TCP keepalive with system defaults if overrides isn't specified.
        /// </summary>
        public virtual bool TcpKeepAlive
        {
            get => _tcpKeepAlive;
            set
            {
                base[TcpKeepAliveKeyword] = (_tcpKeepAlive = value);
            }
        }

        /// <summary>
        /// Determines the size of socket read buffer.
        /// </summary>
        public virtual int SocketReceiveBufferSize
        {
            get => _socketReceiveBufferSize;
            set
            {
                base[SocketReceiveBufferSizeKeyword] = (_socketReceiveBufferSize = value);
            }
        }

        /// <summary>
        /// Determines the size of socket send buffer.
        /// </summary>
        public virtual int SocketSendBufferSize
        {
            get => _socketSendBufferSize;
            set
            {
                base[SocketSendBufferSizeKeyword] = (_socketSendBufferSize = value);
            }
        }

         /// <summary>
        ///     Gets a collection containing the keys used by the connection string.
        /// </summary>
        /// <value>A collection containing the keys used by the connection string.</value>
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

        /// <summary>
        ///     Initializes a new instance of the <see cref="DqliteConnectionStringBuilder" /> class.
        /// </summary>
        public DqliteConnectionStringBuilder()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="DqliteConnectionStringBuilder" /> class.
        /// </summary>
        /// <param name="connectionString">
        ///     The initial connection string the builder will represent. Can be null.
        /// </param>
        public DqliteConnectionStringBuilder(string connectionString)
            => ConnectionString = connectionString;

        /// <summary>
        ///     Gets or sets the value associated with the specified key.
        /// </summary>
        /// <param name="keyword">The key.</param>
        /// <returns>The value.</returns>
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
                    case Keywords.Nodes:
                        if(value is IEnumerable<string> nodes)
                        {
                            Nodes = nodes.ToArray();
                        }
                        else if(value is string nodeString)
                        {
                            Nodes = nodeString?.Split(new char[]{','}, StringSplitOptions.RemoveEmptyEntries);
                        }
                        return;
                    case Keywords.DataSource:
                        DataSource = Convert.ToString(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.TcpKeepAliveKeyword:
                        TcpKeepAlive = ConvertToNullableBoolean(value) ?? false;
                        return;

                    case Keywords.SocketReceiveBufferSize:
                        SocketReceiveBufferSize = ConvertToInt(value);
                        return;

                    case Keywords.SocketSendBufferSize:
                        SocketSendBufferSize = ConvertToInt(value);
                        return;

                    default:
                        Debug.Assert(false, "Unexpected keyword: " + keyword);
                        return;
                }
            }
        }
        
        private static int ConvertToInt(object value)
        {
            if(value == null)
            {
                return 0;
            }
            else if(value is string stringValue && int.TryParse(stringValue, out var intValue))
            {
                return intValue;
            }

            return Convert.ToInt32(value);
        }

        private static TEnum ConvertToEnum<TEnum>(object value)
            where TEnum : struct
        {
            if (value is string stringValue)
            {
                return (TEnum)Enum.Parse(typeof(TEnum), stringValue, ignoreCase: true);
            }

            TEnum enumValue;
            if (value is TEnum)
            {
                enumValue = (TEnum)value;
            }
            else if (value.GetType().IsEnum)
            {
                throw new ArgumentException(Resources.ConvertFailed(value.GetType(), typeof(TEnum)));
            }
            else
            {
                enumValue = (TEnum)Enum.ToObject(typeof(TEnum), value);
            }

            if (!Enum.IsDefined(typeof(TEnum), enumValue))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(value),
                    value,
                    Resources.InvalidEnumValue(typeof(TEnum), enumValue));
            }

            return enumValue;
        }

        private static bool? ConvertToNullableBoolean(object value)
        {
            if (value == null
                || (value is string stringValue && stringValue.Length == 0))
            {
                return null;
            }

            return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        ///     Clears the contents of the builder.
        /// </summary>
        public override void Clear()
        {
            base.Clear();

            for (var i = 0; i < _validKeywords.Count; i++)
            {
                Reset((Keywords)i);
            }
        }

        /// <summary>
        ///     Determines whether the specified key is used by the connection string.
        /// </summary>
        /// <param name="keyword">The key to look for.</param>
        /// <returns>true if it is use; otherwise, false.</returns>
        public override bool ContainsKey(string keyword)
            => _keywords.ContainsKey(keyword);

        /// <summary>
        ///     Removes the specified key and its value from the connection string.
        /// </summary>
        /// <param name="keyword">The key to remove.</param>
        /// <returns>true if the key was used; otherwise, false.</returns>
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

        /// <summary>
        ///     Determines whether the specified key should be serialized into the connection string.
        /// </summary>
        /// <param name="keyword">The key to check.</param>
        /// <returns>true if it should be serialized; otherwise, false.</returns>
        public override bool ShouldSerialize(string keyword)
            => _keywords.TryGetValue(keyword, out var index) && base.ShouldSerialize(_validKeywords[(int)index]);

        /// <summary>
        ///     Gets the value of the specified key if it is used.
        /// </summary>
        /// <param name="keyword">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>true if the key was used; otherwise, false.</returns>
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
                case Keywords.Nodes:
                    return Nodes != null ? string.Join(",",Nodes) : null;

                case Keywords.DataSource:
                    return DataSource;

                case Keywords.TcpKeepAliveKeyword:
                    return TcpKeepAlive;

                case Keywords.SocketReceiveBufferSize:
                    return SocketReceiveBufferSize;

                case Keywords.SocketSendBufferSize:
                    return SocketSendBufferSize;

                default:
                    Debug.Assert(false, "Unexpected keyword: " + index);
                    return null;
            }
        }

        private static Keywords GetIndex(string keyword)
            => !_keywords.TryGetValue(keyword, out var index)
                ? throw new ArgumentException(Resources.KeywordNotSupported(keyword))
                : index;

        private void Reset(Keywords index)
        {
            switch (index)
            {
                case Keywords.Nodes:
                    _nodes = new string[0];
                    return;
                case Keywords.DataSource:
                    _dataSource = string.Empty;
                    return;
                case Keywords.TcpKeepAliveKeyword:
                    _tcpKeepAlive = false;
                    return;
                case Keywords.SocketReceiveBufferSize:
                    _socketReceiveBufferSize = 0;
                    return;
                case Keywords.SocketSendBufferSize:
                    _socketSendBufferSize = 0;
                    return;
                default:
                    Debug.Assert(false, "Unexpected keyword: " + index);
                    return;
            }
        }
    }
}
