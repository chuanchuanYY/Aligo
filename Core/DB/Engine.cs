using CommunityToolkit.Diagnostics;
using Core.Common;
using Core.Data;
using Core.IO;
using Microsoft.Extensions.Logging;

namespace Core.DB;

public class Engine
{
    private const string DataFileExtension = ".data";
    private readonly EngineOptions _options;
    private DataFile _activeFile;
    private readonly Dictionary<UInt32, DataFile> _olderFiles = new();
    private readonly  IIndexer _index;

    private readonly ILogger<Engine> _logger;
    public Engine(EngineOptions options)
    {
        _options = options;
        _index = new DictionaryIndexer();
        _logger = Log.Factory.CreateLogger<Engine>();
        Init();
    }

    private void Init()
    {
       
        // 判断数据库目录是否已经存在
        if (!Path.Exists(_options.DirPath))
        {
            try
            {
                Directory.CreateDirectory(_options.DirPath);
            }
            catch (Exception e)
            {
               _logger.LogError(e, "Failed to create directory");
                throw;
            }
        }
        
        // 加载数据文件
        List<DataFile> datafiles = LoadDataFiles();
        if (datafiles.Count == 0)
        {
            return;
        }

        if (datafiles.Count > 1)
        {
            for (int i = 0; i < datafiles.Count -1; i++)
            {
                var datafile = datafiles[i];
                _olderFiles.Add(datafile.FileID,datafile);
            }
        }
        _activeFile = datafiles.Last();
        
        // 加载内存索引
        LoadIndexer(datafiles);
    }

    private void LoadIndexer(List<DataFile> dataFiles)
    {
        foreach (var dataFile in dataFiles)
        {
            UInt64 offset = 0;
            // 循环加载数据文件中的所有记录
            while (true)
            {
                var logRecord = dataFile.ReadLogRecord(offset);
                if (logRecord == null)
                {
                    // 读到最后一条数据了
                    break;
                }
                
                
                // 虽然可以直接判断 RecordType == Normal 再添加，这样就不用添加再删除了。
                // 但只有put是覆盖旧的value的时候才可行.
                // 否则新的记录将无法覆盖旧的记录导致加载错误的索引.
                // 因为读取Datafile是从小到大读的(升序),也就是从最旧的datafile开始加载数据
                
                _index.Put(logRecord.Key,new LogRecordPos(dataFile.FileID,offset));
                if (logRecord.RecordType == LogRecordType.Deleted)
                {
                    if (!_index.Delete(logRecord.Key))
                    {
                        ThrowHelper.ThrowInvalidOperationException("failed to delete key ");
                    }
                }
                offset += dataFile.WriteOffset;
            }
        }
    }
    private List<DataFile> LoadDataFiles()
    {
        return Directory.GetFiles(_options.DirPath)
            .Select(Path.GetFileName)
            .Where(f=> Path.HasExtension(f) && Path.GetExtension(f).Equals(DataFileExtension))
            .Select(f =>
            {
                if(UInt32.TryParse(f.Split(".")[0],out var fileID))
                {
                    ThrowHelper.ThrowExternalException("datafile 被篡改");
                }
                IIOManager io = new FileIO(f);
                return  new DataFile(fileID, io);
            })
            .OrderBy(datafile => datafile.FileID)
            .ToList();
    }


    public bool Delete(byte[] key)
    {
        Guard.IsNotNull(key);
        
        // 先从内存索引查看是否包含该key的索引
        LogRecordPos? recordPos = _index.Get(key);
        if (recordPos == null)
        {
            return false;
        }
        
        // 删除的方式为追加一条ReocrdType类型为Deleted的数据表示删除(软删除)
        var record = new LogRecord()
        {
            Key = key,
            Value = [],
            RecordType = LogRecordType.Deleted
        };

        AppendLogRecord(record);
        _index.Delete(key);
        
        return true;
    }
    public bool Put(byte[] key, byte[] value)
    {
        Guard.IsNotNull(key);
        Guard.IsNotNull(value);
        
        // 构造LogRecord
        var record = new LogRecord()
        {
            Key = key,
            Value = value,
            RecordType = LogRecordType.Normal
        };
        
        // 追加写入记录
        LogRecordPos recordPos = AppendLogRecord(record);
        
        // 记录内存索引
        return _index.Put(key, recordPos);
    }

    private LogRecordPos AppendLogRecord(LogRecord record)
    {
        Guard.IsNotNull(record);
        
        var encodedRecord = record.Encode();
        
        // 判断写入是否会超出阈值
        if (_activeFile.WriteOffset + (ulong)encodedRecord.Length > _options.DataFileSize)
        {
            // 持久化当前活跃文件
            _activeFile.Sync();
            
            // 创建新的数据文件
            string fileName = Path.Join(_options.DirPath, _activeFile.FileID.ToString());
            var dataFile = new DataFile(_activeFile.FileID + 1, new FileIO(fileName));
            
            // 将当前获取文件添加到就的数据文件中
            _olderFiles.Add(_activeFile.FileID,_activeFile);
            
            // 将当前活跃文件切换为新创建的数据文件
            _activeFile = dataFile;
            
            //  记录写入的偏移量
            var offset = _activeFile.WriteOffset;
            _activeFile.Write(encodedRecord);

            if (_options.AlwaySync)
            {
                _activeFile.Sync();
            }

            return new LogRecordPos(_activeFile.FileID,offset);
        }
        else
        {
            var offset = _activeFile.WriteOffset;
            _activeFile.Write(encodedRecord);
            
            if (_options.AlwaySync)
            {
                _activeFile.Sync();
            }
            return new LogRecordPos(_activeFile.FileID,offset);
        }
    }

    public byte[] Get(byte[] key)
    {
        Guard.IsNotNull(key);

        LogRecordPos? recordPos = _index.Get(key);
        // 如果recordPos为空，表示找不到数据
        if (recordPos == null)
        {
            ThrowHelper.ThrowArgumentException("key not found");
        }
        
        // 从数据文件获取 Value
        LogRecord? record;
        if (recordPos.FileID == _activeFile.FileID)
        {
            record = _activeFile.ReadLogRecord(recordPos.Offset);
            if (record == null)
            {
                throw new Exception($"Failed to read logRecord ");
            }
        }
        else
        {
            if (!_olderFiles.ContainsKey(recordPos.FileID))
            {
                ThrowHelper.ThrowInvalidOperationException("key not found in data file");
            }

            var datafile = _olderFiles[recordPos.FileID];
            record = datafile.ReadLogRecord(recordPos.Offset);
            if (record == null)
            {
                throw new Exception($"Failed to read logRecord ");
            }
            if (record.RecordType != LogRecordType.Normal)
            {
                ThrowHelper.ThrowInvalidDataException("Record type is not normal");
            }
        }
        
        return record.Value;
    }
}