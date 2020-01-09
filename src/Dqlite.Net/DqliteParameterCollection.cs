using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dqlite.Net
{
    public class DqliteParameterCollection : DbParameterCollection
    {
        private readonly List<DqliteParameter> parameters;

        protected internal DqliteParameterCollection()
        {
            this.parameters = new List<DqliteParameter>();
        }

        public override int Count
            => parameters.Count;

        public override object SyncRoot
            => ((ICollection)parameters).SyncRoot;

        public new virtual DqliteParameter this[int index]
        {
            get => parameters[index];
            set
            {
                if (parameters[index] == value)
                {
                    return;
                }

                parameters[index] = value;
            }
        }

        public new virtual DqliteParameter this[string parameterName]
        {
            get => this[IndexOfChecked(parameterName)];
            set => this[IndexOfChecked(parameterName)] = value;
        }

        public override int Add(object value)
        {
            parameters.Add((DqliteParameter)value);

            return Count - 1;
        }

        public virtual DqliteParameter Add(DqliteParameter value)
        {
            parameters.Add(value);

            return value;
        }

        public override void AddRange(Array values)
            => AddRange(values.Cast<DqliteParameter>());

        public virtual void AddRange(IEnumerable<DqliteParameter> values)
            => parameters.AddRange(values);

        public virtual DqliteParameter AddWithValue(string parameterName, object value)
        {
            var parameter = new DqliteParameter(parameterName, value);
            Add(parameter);

            return parameter;
        }

        public virtual DqliteParameter AddWithValue(object value)
        {
            var parameter = new DqliteParameter(value);
            Add(parameter);

            return parameter;
        }

        public override void Clear()
            => parameters.Clear();

        public override bool Contains(object value)
            => Contains((DqliteParameter)value);

        public virtual bool Contains(DqliteParameter value)
            => parameters.Contains(value);

        public override bool Contains(string value)
            => IndexOf(value) != -1;

        public override void CopyTo(Array array, int index)
            => CopyTo((DqliteParameter[])array, index);

        public virtual void CopyTo(DqliteParameter[] array, int index)
            => parameters.CopyTo(array, index);

        public override IEnumerator GetEnumerator()
            => parameters.GetEnumerator();

        protected override DbParameter GetParameter(int index)
            => this[index];

        protected override DbParameter GetParameter(string parameterName)
            => GetParameter(IndexOfChecked(parameterName));

        public override int IndexOf(object value)
            => IndexOf((DqliteParameter)value);

        public virtual int IndexOf(DqliteParameter value)
            => parameters.IndexOf(value);

        public override int IndexOf(string parameterName)
        {
            for (var index = 0; index < parameters.Count; index++)
            {
                if (parameters[index].ParameterName == parameterName)
                {
                    return index;
                }
            }

            return -1;
        }

        public override void Insert(int index, object value)
            => Insert(index, (DqliteParameter)value);

        public virtual void Insert(int index, DqliteParameter value)
            => parameters.Insert(index, value);

        public override void Remove(object value)
            => Remove((DqliteParameter)value);

        public virtual void Remove(DqliteParameter value)
            => parameters.Remove(value);

        public override void RemoveAt(int index)
            => parameters.RemoveAt(index);

        public override void RemoveAt(string parameterName)
            => RemoveAt(IndexOfChecked(parameterName));

        protected override void SetParameter(int index, DbParameter value)
            => this[index] = (DqliteParameter)value;

        protected override void SetParameter(string parameterName, DbParameter value)
            => SetParameter(IndexOfChecked(parameterName), value);

        private int IndexOfChecked(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index == -1)
            {
                throw new IndexOutOfRangeException("Parameter Not Found");
            }

            return index;
        }

        public DqliteParameter[] ToArray()
        {
            return this.parameters.ToArray();
        }

    }
}
