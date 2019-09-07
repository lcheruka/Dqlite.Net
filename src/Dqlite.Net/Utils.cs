using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    internal static class Utils
    {

        public static ulong PadWord(ulong value)
        {
            if (value % 8 != 0)
            {
                value += (8 - (value % 8));
            }
            return value;
        }

        public static int PadWord(int value)
        {
            if(value == 0)
            {
                return 8;
            }
            else if (value % 8 != 0)
            {
                value += (8 - (value % 8));
            }
            return value;
        }

        public static int GetSize(DqliteParameter[] parameters)
        {
            var headerBits = 8 + parameters.Length * 8;
            var padBits = 0;
            if(headerBits % 64 != 0)
            {
                padBits = 64 - (headerBits % 64);
            }

            var length = (headerBits + padBits) / 64 * 8;
            foreach (var parameter in parameters)
            {
                length += PadWord(parameter.Size);
            }

            return length;
        }
    }
}    
