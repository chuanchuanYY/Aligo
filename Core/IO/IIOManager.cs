namespace Core.IO;

public interface IIOManager
{
    UInt64 Read(byte[] buf, UInt64 offset);
    UInt64 Write(byte[] buf);
    void Sync();
}