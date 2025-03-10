using System.Buffers;
using System.IO.Hashing;
using CommunityToolkit.Diagnostics;
using CommunityToolkit.HighPerformance.Buffers;
using Core.Common;
using Core.Data;
using Core.IO;
using Microsoft.Extensions.Logging;

namespace Core.DB;

public class DataFile
{
    public UInt32 FileID { get; private set; }
    public UInt64 WriteOffset { get;set; } = 0;
    private readonly IIOManager _ioManager;
    private readonly ILogger<DataFile> _logger;
    public DataFile(UInt32 fileID,IIOManager ioManager)
    {
        FileID = fileID;
        _ioManager = ioManager;
        _logger = Log.Factory.CreateLogger<DataFile>();
    }

    public bool Write(byte[] buf)
    {
       var writeCount = _ioManager.Write(buf);
       if (writeCount != (ulong)buf.Length)
       {
           return false;
       }
       return true;
    }

    public void Sync()
    {
        _ioManager.Sync();
    }

    public LogRecord? ReadLogRecord(UInt64 offset)
    {
        byte[] headerBuf = new byte[LogRecord.MaxHeaderSize()];

        if (_ioManager.Read(headerBuf, offset) != (ulong)headerBuf.Length)
        {
            _logger.LogError($"Read log record {offset} failed.");
            ThrowHelper.ThrowInvalidOperationException("Failed to read log header ");
        }
        
        // 获取KeySize 和 valueSize
        var headerSpan = headerBuf.AsSpan();
        
        UInt32 keySize = UInt32.Parse(headerSpan.Slice(1,4));
        UInt32 valueSize = UInt32.Parse(headerSpan.Slice(5,4));
        // 获取完整记录
        var keyValueBuf = ArrayPool<byte>.Shared.Rent((int)(keySize + valueSize + 4));
        
        if (_ioManager.Read(keyValueBuf, offset + (ulong)headerSpan.Length) != (ulong)keyValueBuf.Length)
        {
            ArrayPool<byte>.Shared.Return(keyValueBuf);
            _logger.LogError($"Read log record {offset} failed.");
            ThrowHelper.ThrowInvalidOperationException("Failed to read log header ");
        }

        var recordBuf = new byte[headerBuf.Length + keyValueBuf.Length];
        headerBuf.CopyTo(recordBuf,0);
        keyValueBuf.CopyTo(recordBuf,headerBuf.Length);
        
        LogRecord record = LogRecord.Decode(recordBuf);
        ArrayPool<byte>.Shared.Return(keyValueBuf);
        return record;
    }
}