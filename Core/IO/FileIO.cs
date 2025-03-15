using CommunityToolkit.Diagnostics;
using Core.Common;
using Microsoft.Extensions.Logging;

namespace Core.IO;

internal class FileIO: IIOManager
{
    private readonly FileStream _readStream;
    private readonly FileStream _writeStream;
    private readonly ILogger<FileIO> _logger;
    public bool AlwayFlush { get; set; } = false;
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="filePath"></param>
    public FileIO(string filePath)
    {
        Guard.IsNotNullOrEmpty(filePath);
        _logger = Log.Factory.CreateLogger<FileIO>();
        // Append access can be requested only in write-only mode. (Parameter 'access')

        try
        {
            _readStream = new FileStream(filePath,
                FileMode.OpenOrCreate,
                FileAccess.Read, FileShare.ReadWrite);
            _writeStream = new FileStream(filePath,
                FileMode.Append,
                FileAccess.Write,FileShare.ReadWrite);

        }
        catch (Exception e)
        {
            _logger.LogError(e,"Failed to Open File");
            throw;
        }
  
    }
    
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="buf">将数据读取到字节缓冲区，读取的大小维缓冲区的长度</param>
    /// <param name="offset">读取偏移量，将从文件的offset偏移量开始读取数据</param>
    /// <returns></returns>
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
    /// <param name="buf">将字节数组全部追加写入到文件</param>
    /// <returns></returns>
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
        _writeStream.Close();
        _readStream.Close();
        
        _readStream.Dispose();
        _writeStream.Dispose();
    }
}