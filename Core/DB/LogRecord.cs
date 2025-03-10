using System.IO.Hashing;
using System.Text;
using CommunityToolkit.Diagnostics;

namespace Core.DB;

public class LogRecord
{
    public byte[] Key { get; set; }
    public byte[] Value { get; set; }
    public LogRecordType RecordType { get; set; }

    /// <summary>
    /// 编码当前LogRecord为字节数组
    /// </summary>
    /// <returns></returns>
    public byte[] Encode()
    {
        // +----------header------------+
        // | type | keySize | valueSize | Key | Value | crc |
        using var memStream = new MemoryStream();
        using var writer = new BinaryWriter(memStream);
        writer.Write((byte)RecordType);
        writer.Write(Key.Length);
        writer.Write(Value.Length);
        writer.Write(Key);
        writer.Write(Value);

        var record = memStream.ToArray();
        var crc = Crc32.HashToUInt32(record);
        writer.Write(crc);
        
        return memStream.ToArray();
    }
    
    public static LogRecord Decode(byte[] data)
    {
       using var memStream = new MemoryStream(data);
       using var reader = new BinaryReader(memStream);

       var type = reader.ReadByte();
       var keySize = reader.ReadUInt32();
       var valueSize = reader.ReadUInt32();
       var keyBuf = new byte[keySize];
       var valueBuf = new byte[valueSize];
      
       if (reader.Read(keyBuf) != keySize)
       {
           throw new Exception("decode key error");
       }
       
       if (reader.Read(valueBuf) != valueSize)
       {
           throw new Exception("decode value error");
       }
       
       // check crc 
       var crc =reader.ReadUInt32();
       if (Crc32.HashToUInt32(data.Take(data.Length - 4).ToArray()) != crc)
       {
           ThrowHelper.ThrowInvalidDataException("crc error");
       }

       return new LogRecord()
       {
           Key = keyBuf,
           Value = valueBuf,
           RecordType = (LogRecordType)type,
       };
    }

    public static int MaxHeaderSize()
    {
        // | type | keySize | valueSize |  
        return sizeof(byte) + sizeof(UInt32) + sizeof(UInt32);
    }
}