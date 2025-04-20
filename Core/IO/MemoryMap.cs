using System.IO.MemoryMappedFiles;
using CommunityToolkit.Diagnostics;
using Core.Common;
using Core.Exceptions;
using Microsoft.Extensions.Logging;

namespace Core.IO;

public class MemoryMap: IIOManager
{

    private readonly ILogger<MemoryMap> _logger;
    private readonly MemoryMappedFile _mapFile;
    private readonly MemoryMappedViewAccessor _mapAccessor;
    private long _writePosition = 0;
    public MemoryMap(string filePath)
    {
        Guard.IsNotNullOrEmpty(filePath);
        _logger = Log.Factory.CreateLogger<MemoryMap>();
        try
        {
            FileInfo fileInfo = new FileInfo(filePath);
            _mapFile = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
            _mapAccessor = _mapFile.CreateViewAccessor(0,fileInfo.Length);
        }
        catch (Exception e)
        {
            _logger.LogError(e,ErrorMessage.OpenFileError);
            throw new IOException(ErrorMessage.OpenFileError);
        }
    }
    public void Dispose()
    {
        _mapAccessor.Dispose();
        _mapFile.Dispose();
        
    }

    public ulong Read(byte[] buf, ulong offset)
    {
        try
        {
           var result =  (ulong)_mapAccessor.ReadArray((long)offset,buf,0,buf.Length);
           return result;
        }
        catch (Exception e)
        {
           _logger.LogError(e,ErrorMessage.ReadFileError);
            throw new IOException(ErrorMessage.ReadFileError);
        }
    }

    public ulong Write(byte[] buf)
    {
      throw new NotImplementedException();
    }

    public void Sync()
    {
        try
        {
            _mapAccessor.Flush();
        }
        catch (Exception e)
        {
            _logger.LogError(e,ErrorMessage.SyncError);
            throw new IOException(ErrorMessage.SyncError);
        }
    }
}