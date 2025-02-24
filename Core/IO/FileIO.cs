using CommunityToolkit.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Core.IO;

public class FileIO: IIOManager,IDisposable
{
    private readonly FileStream _fileStream;
    private readonly ILogger<FileIO> _logger;
    public bool AlwayFlush { get; set; } = true;
  
    public FileIO(string filePath,ILogger<FileIO> logger)
    {
        Guard.IsNotNullOrEmpty(filePath);
        Guard.IsNotNull(logger); 
        _logger = logger;
        _fileStream = new FileStream(filePath,
                        FileMode.OpenOrCreate,
                        FileAccess.ReadWrite, FileShare.ReadWrite);
        
    }
    public ulong Read(byte[] buf, ulong offset)
    {
        Guard.IsNotNull(buf);
        try
        {
             _fileStream.Position = (long)offset;
             return (ulong)_fileStream.Read(buf,0,buf.Length);
        }
        catch (Exception e)
        {
            _logger.LogError(e,"read file failed");
            throw;
        }
    }

    public ulong Write(byte[] buf)
    {
        Guard.IsNotNull(buf);
        try
        {
            _fileStream.Write(buf);
            _fileStream.Flush(AlwayFlush);
            return (ulong)buf.Length;
        }
        catch (Exception e)
        {
           _logger.LogError(e,"write file failed");
            throw;
        }
    }

    public void Sync()
    {
        try
        {
            _fileStream.Flush();
        }
        catch (Exception e)
        {
            _logger.LogError(e,"sync file failed");
            throw;
        }
    }

    public void Dispose()
    {
        _fileStream.Dispose();
    }
}