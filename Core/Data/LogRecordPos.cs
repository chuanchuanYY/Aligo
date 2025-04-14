namespace Core.Data;

public class LogRecordPos
{
    public UInt32 FileID { get; set; }
    public UInt64 Offset { get; set; }

    public LogRecordPos(UInt32 fileId,UInt64 offset)
    {
        this.FileID = fileId;
        this.Offset = offset;
    }


    public static byte[] Encode(LogRecordPos logRecordPos)
    {
        var bytes = new byte[sizeof(UInt32) + sizeof(UInt64)];
        BitConverter.GetBytes(logRecordPos.FileID).CopyTo(bytes,0);
        BitConverter.GetBytes(logRecordPos.Offset).CopyTo(bytes,sizeof(UInt32));
        return bytes;
    }

    public static LogRecordPos Decode(byte[] pos)
    {
        UInt32 fileId = BitConverter.ToUInt32(pos,0);
        UInt64 offset = BitConverter.ToUInt32(pos,sizeof(UInt32));
        
        return new LogRecordPos(fileId,offset);
    }
}