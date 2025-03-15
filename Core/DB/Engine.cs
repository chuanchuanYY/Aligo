using System.Collections;
using CommunityToolkit.Diagnostics;
using Core.Common;
using Core.Data;
using Core.IO;
using Microsoft.Extensions.Logging;

namespace Core.DB;

public class Engine : IEnumerable<KeyValuePair<byte[],LogRecordPos>>,IDisposable
{
    private const string DataFileExtension = ".data";
    private readonly EngineOptions _options;
    private DataFile _activeFile;
    private readonly Dictionary<UInt32, DataFile> _olderFiles = new();
    internal readonly  IIndexer Index;

    // 事务序列号默认从 1开始
    private UInt32 _transactionNumber = 1;
    // 表示非事务
    private const UInt32 NormalNumber = 0; 
    public UInt32 TransactionNumber => _transactionNumber;
    
    internal  readonly  Mutex CommitLock = new Mutex();
    private readonly ILogger<Engine> _logger;
    public Engine(EngineOptions options)
    {
        _options = options;
        Index = new DictionaryIndexer();
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
            string fileName = Path.Join(_options.DirPath, "0" + DataFileExtension);
            _activeFile = new DataFile(0,new FileIO(fileName));
            return;
        }

        if (datafiles.Count >= 1)
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
        // key: 事务序列号 value：同一批事务序列号的记录
        var transactionRecords = new Dictionary<UInt32, List<LogRecord>>();
        
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
                
                // 解析出事务序列号和Key
                var (transactionNo,key) = ParseTransactionNumberAndKey(logRecord.Key);
                if (transactionNo >= _transactionNumber)
                {
                    _transactionNumber = transactionNo + 1;
                }
                // 如果非事务数据直接添加
                if (transactionNo == NormalNumber)
                {   
                    // 使用将事务序列号去除的key
                    var record = new LogRecord()
                    {
                        Key = key,
                        Value = logRecord.Value,
                        RecordType = logRecord.RecordType,

                    };
                    AddIndex(record,dataFile,offset);
                }
                else
                {
                    // 如果是事务结束标记
                    if (logRecord.RecordType == LogRecordType.TransactionFinished)
                    {
                        // 获取该事务的记录
                        var records = transactionRecords[transactionNo];
                        foreach (var record in records)
                        {
                            AddIndex(record,dataFile,offset);
                        }
                    }
                    else
                    {
                        // 先将事务记录暂存，如果有事务完成标识才表示数据可用
                        if (!transactionRecords.ContainsKey(transactionNo))
                        {
                            var record = new LogRecord()
                            {
                                Key = key,
                                Value = logRecord.Value,
                                RecordType = logRecord.RecordType,
                            };
                            transactionRecords.Add(transactionNo,new List<LogRecord>(){ record});
                        }
                        else
                        {
                            transactionRecords[transactionNo].Add(logRecord);
                        }
                    }
                 
                }
                
                // 跟新便宜量指向下一条数据，header + key + value + crc
                offset += (UInt64)(LogRecord.MaxHeaderSize() + logRecord.Key.Length + logRecord.Value.Length + 4);
            }
        }
    }

    private void AddIndex(LogRecord logRecord,DataFile dataFile,UInt64 offset)
    {
        Index.Put(logRecord.Key,new LogRecordPos(dataFile.FileID,offset));
        if (logRecord.RecordType == LogRecordType.Deleted)
        {
            if (!Index.Delete(logRecord.Key))
            {
                ThrowHelper.ThrowInvalidOperationException("failed to delete key ");
            }
        }
    }

    private (UInt32,byte[]) ParseTransactionNumberAndKey(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream);
        var transactionNo = reader.ReadUInt32();
        var key = reader.ReadBytes(bytes.Length - sizeof(UInt32));
        
        return (transactionNo, key);
    }
    private List<DataFile> LoadDataFiles()
    {   
        var files = Directory.GetFiles(_options.DirPath);
        
        List<DataFile> dataFiles = new();
        // 检查文件
        foreach (var f in files)
        {
            // 检测拓展名
            var extension = Path.GetExtension(f);
            if(!string.Equals(extension,DataFileExtension))
            {
                ThrowHelper.ThrowInvalidDataException("Data file corruption");
            }

            var fileID = Convert.ToUInt32(Path.GetFileNameWithoutExtension(f));
            IIOManager io = new FileIO(f);
            var datafile = new DataFile(fileID,io);
            dataFiles.Add(datafile);
        }
        
        return dataFiles.OrderBy(f=> f.FileID).ToList();
    }


    public bool Contains(byte[] key) => Index.Contains(key);
    
    // 返回递增前的值
    internal UInt32 IncrementTransactionNumber()
    {
        var result = _transactionNumber;
        Interlocked.Increment(ref _transactionNumber);
        return result;
    }
    
    
    //
    public WriteBatch CreateWriteBatch(WriteBatchOptions options)
    {
        return new WriteBatch(this,options);
    }
    public bool Delete(byte[] key)
    {
        Guard.IsNotNull(key);
        
        // 先从内存索引查看是否包含该key的索引
        LogRecordPos? recordPos = Index.Get(key);
        if (recordPos == null)
        {
            return false;
        }
        
        // 删除的方式为追加一条ReocrdType类型为Deleted的数据表示删除(软删除)
        var record = new LogRecord()
        {
            Key = EncodeKeyWithTransactionNo(key,NormalNumber),
            Value = [],
            RecordType = LogRecordType.Deleted
        };

        AppendLogRecord(record);
        Index.Delete(key);
        
        return true;
    }
    public bool Put(byte[] key, byte[] value)
    {
        Guard.IsNotNull(key);
        Guard.IsNotNull(value);
        
        // 构造LogRecord
        var record = new LogRecord()
        {
            Key = EncodeKeyWithTransactionNo(key,NormalNumber),
            Value = value,
            RecordType = LogRecordType.Normal
        };
        
        // 追加写入记录
        LogRecordPos recordPos = AppendLogRecord(record);
        
        // 记录内存索引
        return Index.Put(key, recordPos);
    }

    internal LogRecordPos AppendLogRecord(LogRecord record)
    {
        Guard.IsNotNull(record);
        
        var encodedRecord = record.Encode();
        
        // 判断写入是否会超出阈值
        if (_activeFile.WriteOffset + (ulong)encodedRecord.Length > _options.DataFileSize)
        {
            // 持久化当前活跃文件
            _activeFile.Sync();
            
            // 创建新的数据文件
            string fileName = Path.Join(_options.DirPath, _activeFile.FileID.ToString() + DataFileExtension);
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

        LogRecordPos? recordPos = Index.Get(key);
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
                throw new Exception($"Failed to read logRecord at offset {recordPos.Offset}");
            }
        }
        else
        {
          
            var datafile = _olderFiles[recordPos.FileID];
            record = datafile.ReadLogRecord(recordPos.Offset);
            if (record == null)
            {
                throw new Exception($"Failed to read logRecord at offset {recordPos.Offset}");
            }
            if (record.RecordType != LogRecordType.Normal)
            {
                ThrowHelper.ThrowInvalidDataException("Record type is not normal");
            }
        }
       
        return record.Value;
    }


    public IEnumerable<byte[]> GetKeys() => Index.GetKeys();
    public IEnumerator<KeyValuePair<byte[], LogRecordPos>> GetEnumerator()
    {
        return Index.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return this.GetEnumerator();
    }
    // 将事务序列号编码到Key中
    internal byte[] EncodeKeyWithTransactionNo(byte[] key,UInt32 number)
    {
        var buf = new byte[key.Length + sizeof(UInt32)];
        using var stream = new MemoryStream(buf);
        using var writer = new BinaryWriter(stream);
        writer.Write(number);
        writer.Write(key);
        return stream.ToArray();
    }
    public void Sync()
    {
        _activeFile.Sync();
    }

    public void Dispose()
    {
        _activeFile.Sync();
        _activeFile.Dispose();
        foreach (var datafile in _olderFiles.Values)
        {
            datafile.Dispose();
        }
        
    }
}