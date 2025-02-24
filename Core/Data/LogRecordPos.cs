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
}