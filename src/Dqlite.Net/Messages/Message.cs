using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;

namespace Dqlite.Net.Messages
{
    public class Message
    {
        public int Size { get; set; }
        public byte Type { get; set; }
        public byte Revision {get; set; }
        public ushort Unused { get; set; }
        public byte[] Data { get; set; }
        
        public Message()
        {

        }

        public Message(byte type, byte revision, byte[] data)
        {
            this.Type = type;
            this.Revision = revision;

            if(data.Length % 8 != 0)
            {
                this.Data = new byte[data.Length + (8 - this.Size % 8)];
                Buffer.BlockCopy(data, 0, this.Data, 0, data.Length);
            }
            else
            {
                this.Data = data;
            }

            this.Size = this.Data.Length / 8;
        }       
    }
}