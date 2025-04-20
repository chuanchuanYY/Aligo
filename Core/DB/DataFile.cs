using System.Buffers;
using System.IO.Hashing;
using CommunityToolkit.Diagnostics;
using CommunityToolkit.HighPerformance.Buffers;
using Core.Common;
using Core.Data;
using Core.Exceptions;
using Core.IO;
using Microsoft.Extensions.Logging;

namespace Core.DB;

internal class DataFile: IDisposable
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
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="buf"></param>
    /// <returns></returns>
    public UInt64 Write(byte[] buf)
    {
        try
        {
            var writeCount = _ioManager.Write(buf);
            WriteOffset += writeCount;
            return writeCount;
        }
        catch (Exception e)
        {
            _logger.LogError(e,ErrorMessage.WriteDataFileError);
            throw;
        }
     
    }
    
    /// <summary>
    /// <exception cref="IOException"></exception>
    /// </summary>
    public void Sync()
    {
        try
        {
            _ioManager.Sync();
        }
        catch (Exception e)
        {
            _logger.LogError(e,ErrorMessage.SyncError);
            throw;
        }
    }

    public LogRecord? ReadLogRecord(UInt64 offset)
    {
        // +----------header------------+
        // | type | keySize | valueSize | Key | Value | crc |
        byte[] headerBuf = new byte[LogRecord.MaxHeaderSize()];
        try
        {
            if (_ioManager.Read(headerBuf, offset) != (ulong)headerBuf.Length)
            {
                return null;
            }
        
            // 获取KeySize 和 valueSize
            var headerSpan = headerBuf.AsSpan();
      
            UInt32 keySize =   BitConverter.ToUInt32(headerSpan.Slice(1, 4));
            UInt32 valueSize = BitConverter.ToUInt32(headerSpan.Slice(5,4));
            // 获取完整记录
            var keyValueBuf =new byte[(int)(keySize + valueSize + 4)];
            if (_ioManager.Read(keyValueBuf, offset + (ulong)headerSpan.Length) != (ulong)keyValueBuf.Length)
            {
                return null;
            }

            var recordBuf = new byte[headerBuf.Length + keyValueBuf.Length];
            headerBuf.CopyTo(recordBuf,0);
            keyValueBuf.CopyTo(recordBuf,headerBuf.Length);
        
            LogRecord record = LogRecord.Decode(recordBuf);
            return record;
        }
        catch (Exception e)
        {
            _logger.LogError(e,ErrorMessage.ReadLogRecordError);
            throw new Exception(ErrorMessage.ReadLogRecordError);
        }
      
    }

    public void Dispose()
    {
        _ioManager.Dispose();
    }
}