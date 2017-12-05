using System;
using System.Data;
using System.Linq;

namespace Dg.Deblazer.Extensions
{
    public static class BinaryExtensions
    {
        public static ulong RowVersionToUInt64(this byte[] binary)
        {
            return RowVersionBytesToUInt64(binary);
        }

        public static ulong RowVersionBytesToUInt64(byte[] bytes)
        {
            // This solution returns sum as int and has lead to overflow
            // int index = bytes.Length;
            // return bytes.Sum(b => b << (--index * 8));

            // reverse the array so that the most important bit is first (bigendian)
            return BitConverter.ToUInt64(bytes.Reverse().ToArray(), 0);
        }

        public static byte[] ToBinary(this ulong x)
        {
            return new byte[] {
                (byte)((x >> 56) & 0xFF),
                (byte)((x >> 48) & 0xFF),
                (byte)((x >> 40) & 0xFF),
                (byte)((x >> 32) & 0xFF),
                (byte)((x >> 24) & 0xFF),
                (byte)((x >> 16) & 0xFF),
                (byte)((x >>  8) & 0xFF),
                (byte)((x >>  0) & 0xFF)
            };
        }
    }
}