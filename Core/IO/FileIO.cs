using CommunityToolkit.Diagnostics;
using Core.Common;
using Microsoft.Extensions.Logging;

namespace Core.IO;

public class FileIO: IIOManager
{
    private readonly FileStream _readStream;
    private readonly FileStream _writeStream;
    private readonly ILogger<FileIO> _logger;
    public bool AlwayFlush { get; set; } = true;
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="filePath"></param>
    /// <exception cref="ArgumentException"></exception>
    /// <exception cref="ArgumentNullException"></exception>
    public FileIO(string filePath)
    {
        Guard.IsNotNullOrEmpty(filePath);
        _logger = Log.Factory.CreateLogger<FileIO>();
        // Append access can be requested only in write-only mode. (Parameter 'access')
        _readStream = new FileStream(filePath,
                        FileMode.OpenOrCreate,
                        FileAccess.Read, FileShare.ReadWrite);
        _writeStream = new FileStream(filePath,
                        FileMode.Append,
                        FileAccess.Write);

    }
    
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="buf"></param>
    /// <param name="offset"></param>
    /// <returns></returns>
    /// <exception cref="Exception">Read File Exception</exception>
    public ulong Read(byte[] buf, ulong offset)
    {
        Guard.IsNotNull(buf);
        try
        {
             _readStream.Position = (long)offset;
             return (ulong)_readStream.Read(buf,0,buf.Length);
        }
        catch (Exception e)
        {
            _logger.LogError(e,"read file failed");
            throw;
        }
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="buf"></param>
    /// <returns></returns>
    /// <exception cref="Exception">Write File Exception</exception>
    public ulong Write(byte[] buf)
    {
        Guard.IsNotNull(buf);
        try
        {
            _writeStream.Write(buf);
            _writeStream.Flush(AlwayFlush);
            return (ulong)buf.Length;
        }
        catch (Exception e)
        {
           _logger.LogError(e,"write file failed");
            throw;
        }
    }
    
    /// <summary>
    /// <exception cref="Exception">Sync Flush Stream Exception</exception>
    /// </summary>
    public void Sync()
    {
        try
        {
            _writeStream.Flush();
        }
        catch (Exception e)
        {
            _logger.LogError(e,"sync file failed");
            throw;
        }
    }

    public void Dispose()
    {
        _readStream.Dispose();
        _writeStream.Dispose();
    }
}