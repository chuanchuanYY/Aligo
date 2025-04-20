using System.Collections;
using System.ComponentModel;
using System.Text;
using CommunityToolkit.Diagnostics;
using Core.Common;
using Core.Data;
using Core.Exceptions;
using Core.IO;
using Microsoft.Extensions.Logging;

namespace Core.DB;

public class Engine : IEnumerable<KeyValuePair<byte[],LogRecordPos>>,IDisposable
{
    private const string DataFileExtension = ".data";
    private const string MergeDirName = "_Merge";
    private const string HintFileName = "_Hint";
    private const string MergeFinishedFileName = "_MergeFinished";
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
    
    // 用于记录统计数据
    private UInt64 _reclaimableSize; // 磁盘可回收的空间，单位字节

    
    
    public Engine(EngineOptions options)
    {
        _options = options;
        Index = new DictionaryIndexer();
        _logger = Log.Factory.CreateLogger<Engine>();
        Init();
    }
    #region Init 
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
               _logger.LogError(e, ErrorMessage.CreateDatabaseDirectoryError);
                throw new Exception(ErrorMessage.CreateDatabaseDirectoryError);
            }
        }

        try
        {
            // 加载Merge数据文件
            LoadMergeDataFiles();
            // 加载数据文件
            List<DataFile> datafiles = LoadDataFiles();
            if (datafiles.Count == 0)
            {
                string fileName = Path.Join(_options.DirPath, "0" + DataFileExtension);
                _activeFile = new DataFile(0,IOManagerFactory.Create(_options.IOType,fileName));
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
        
            // 加载hint索引文件
            if (File.Exists(Path.Join(_options.DirPath + MergeDirName, HintFileName)))
            {
                //var io = new FileIO(Path.Join(_options.DirPath + MergeDirName, HintFileName));
                var io = IOManagerFactory.Create(_options.IOType,Path.Join(_options.DirPath + MergeDirName, HintFileName));
                var hintFile = new DataFile(0,io);
                UInt64 offset = 0;
                while (true)
                {
                    // hint索引文件存储的记录value为LogrecordPos
                    var record = hintFile.ReadLogRecord(offset);
                    if (record == null)
                        break;
                    var (_,key) = ParseTransactionNumberAndKey(record.Key);
                
                    // 解码LogrecordPos
                    var pos = LogRecordPos.Decode(record.Value);
                    Index.Put(key,pos);
                }
            
            }
      
            // 加载内存索引
            LoadIndexer(datafiles);
        
            // todo ...
            // 将Merge文件加载回来后，要删除merge目录的，不然重启又要去复制一遍文件等等
        
        
            // 如果IOManagerType 是 MemoryMap 类型，在初始化完成后将它修改为FileIO
            // MemoryMap只用于加速初始化
            _options.IOType = IOManagerType.FileIO;
        }
        catch (Exception e)
        {
            _logger.LogError(e, ErrorMessage.EngineInitError);
            throw new Exception(ErrorMessage.EngineInitError);
        }
       
    }
    
    /// <summary>
    /// <exception cref="Exception"></exception>
    /// </summary>
    private void LoadMergeDataFiles()
    {
        // 判断Merge目录是否存在
        if (!Directory.Exists(_options.DirPath + MergeDirName))
        {
            return;
        }
        
        // 加载文件
        var files = Directory.GetFiles(_options.DirPath + MergeDirName);
        
        // 判断是否有Merge完成标识文件
        bool hasMerge = false;
        string finishedFilePath = string.Empty;
        foreach (var f in files)
        {
            if (Path.GetFileName(f) == MergeFinishedFileName)
            {
                hasMerge = true;
                finishedFilePath = f;
            }
        }

        if (!hasMerge)
        {
            return;
        }


        try
        {
            // 获取最近未参与Merge的文件ID
            //var io = new FileIO(finishedFilePath);
            var io = IOManagerFactory.Create(_options.IOType, finishedFilePath);
            var finishedDataFile = new DataFile(0,io);
            var finishedRecord = finishedDataFile.ReadLogRecord(0);
        
            var recentFileId = BitConverter.ToUInt32(finishedRecord!.Value);
            // 先删除已经Merge的文件
            // 删除文件id小于最近参与merger文件id的文件
            var willDeleteFiles = Directory.GetFiles(_options.DirPath)
                .Where(f =>
                {
                    if (!UInt32.TryParse(Path.GetFileNameWithoutExtension(f), out var fileId))
                    {
                        return false;
                    }

                    if (fileId < recentFileId)
                    {
                        return true;
                    }
                    return false;
                });
            foreach (var f in willDeleteFiles)
            {
                File.Delete(f);
            }
        
            // 将merge 后的文件复制回引擎目录
            foreach (var f in files)
            {
            
                if (!UInt32.TryParse(Path.GetFileNameWithoutExtension(f), out var fileId))
                {
                    continue;
                }
                File.Copy(f,_options.DirPath + Path.GetFileName(f));
            }

        }
        catch (Exception e)
        {
           _logger.LogError(e,ErrorMessage.LoadMergeDataFilesError);
            throw;
        }
    }
    private void LoadIndexer(List<DataFile> dataFiles)
    {
        // key: 事务序列号 value：同一批事务序列号的记录
        var transactionRecords = new Dictionary<UInt32, List<LogRecord>>();
        bool hasMerge = File.Exists(Path.Join(_options.DirPath + MergeDirName, MergeFinishedFileName))
                        && File.Exists(Path.Join(_options.DirPath + MergeDirName, HintFileName)); // 是否有合并的文件
        UInt32 recentFileId = 0;
        if (hasMerge)
        {
            // 获取最近未参与Merge的文件ID
            //var io = new FileIO(Path.Join(_options.DirPath + MergeDirName, MergeFinishedFileName));
            var io = IOManagerFactory.Create(_options.IOType,
                Path.Join(_options.DirPath + MergeDirName, MergeFinishedFileName));
            var finishedDataFile = new DataFile(0,io);
            var finishedRecord = finishedDataFile.ReadLogRecord(0);
        
            recentFileId = BitConverter.ToUInt32(finishedRecord!.Value);
        }
        
        foreach (var dataFile in dataFiles)
        {
            // 跳过已经从hint文件加载的数据
            if (hasMerge && dataFile.FileID < recentFileId)
            {
                continue;
            }
            
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
    
    
    /// <summary>
    /// 
    /// </summary>
    /// <exception cref="InvalidDataException"></exception>
    /// <returns></returns>
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

            if (!UInt32.TryParse(Path.GetFileNameWithoutExtension(f), out var fileId))
            {
                continue;
            }
            
            IIOManager io = IOManagerFactory.Create(_options.IOType,f);
            var datafile = new DataFile(fileId,io);
            dataFiles.Add(datafile);
        }
        
        return dataFiles.OrderBy(f=> f.FileID).ToList();
    }

    #endregion

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
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <exception cref="KeyNotFoundException"></exception>
    /// <returns>已经删除的Value</returns>
    public byte[] Delete(byte[] key)
    {
        Guard.IsNotNull(key);
        
        // 先从内存索引查看是否包含该key的索引
        LogRecordPos? recordPos = Index.Get(key);
        if (recordPos == null)
        {
            throw new KeyNotFoundException();
        }

        _reclaimableSize += (UInt64)GetLogRecord(key).Encode().Length;

        var oldRecord = GetLogRecord(key);
        // 删除的方式为追加一条ReocrdType类型为Deleted的数据表示删除(软删除)
        var record = new LogRecord()
        {
            Key = EncodeKeyWithTransactionNo(key,NormalNumber),
            Value = [],
            RecordType = LogRecordType.Deleted
        };

        AppendLogRecord(record);
        Index.Delete(key);
        
        return oldRecord.Value;
    }
    
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="Exception"></exception>
    /// <returns>已经添加成功的键值对</returns>
    public KeyValuePair<byte[],byte[]> Put(byte[] key, byte[] value)
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

        bool isExist = Index.Contains(key);

        try
        {
            // 追加写入记录
            LogRecordPos recordPos = AppendLogRecord(record);
            if (isExist)
            {
                _reclaimableSize += (UInt64)GetLogRecord(key).Encode().Length;
            }
            // 记录内存索引
            var putResult =  Index.Put(key, recordPos);
            if (!putResult)
            {
                throw new Exception(ErrorMessage.PutIndexError);
            }

            return new KeyValuePair<byte[], byte[]>(key,value);
        }
        catch (Exception e)
        {
           _logger.LogError(e,ErrorMessage.EnginePutError);
            throw new Exception(ErrorMessage.EnginePutError);
        }
    }

    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="record"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="Exception"></exception>
    internal LogRecordPos AppendLogRecord(LogRecord record)
    {
        Guard.IsNotNull(record);
        
        var encodedRecord = record.Encode();

        try
        {
            // 判断写入是否会超出阈值
            if (_activeFile.WriteOffset + (ulong)encodedRecord.Length > _options.DataFileSize)
            {
                // 持久化当前活跃文件
                _activeFile.Sync();
            
                // 创建新的数据文件
                string fileName = Path.Join(_options.DirPath, _activeFile.FileID.ToString() + DataFileExtension);
                var dataFile = new DataFile(_activeFile.FileID + 1,IOManagerFactory.Create(_options.IOType,fileName));
            
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
        catch (Exception e)
        {
            _logger.LogError(e,ErrorMessage.AppendLogRecordError);
            throw new Exception(ErrorMessage.AppendLogRecordError);
        }
    
    }
   
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="KeyNotFoundException"></exception>
    public byte[] Get(byte[] key)
    {
        Guard.IsNotNull(key);
        return GetLogRecord(key).Value;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    /// <exception cref="KeyNotFoundException"></exception>
    /// <exception cref="Exception"></exception>
    private LogRecord GetLogRecord(byte[] key)
    {
        LogRecord? record;
        LogRecordPos? recordPos = Index.Get(key);
        // 如果recordPos为空，表示找不到数据
        if (recordPos == null)
        {
           throw new KeyNotFoundException();
        }
        
        // 从数据文件获取 Value
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

        return record;
    }
    public void Merge()
    {
        if ((double)_reclaimableSize / (double)DBHelper.GetDirectorySize(_options.DirPath) < _options.ReclaimableRatio)
        {
            return;
        }
        // 减少的 
        UInt64 reducedReclaimableSize = 0;
        // 获取数据文件
        List<DataFile> dataFiles = new();
        IIOManager? io;
        foreach (var (fid,df) in _olderFiles)
        {
           // io = new FileIO(Path.Join(_options.DirPath,fid.ToString() + DataFileExtension));
           io = IOManagerFactory.Create(_options.IOType,Path.Join(_options.DirPath,fid.ToString() + DataFileExtension));
            var datafile = new DataFile(fid,io);
            dataFiles.Add(datafile);
        }

        var activeDataFile = _activeFile;
        activeDataFile.Sync();
       // io = new FileIO(Path.Join(_options.DirPath,activeDataFile.FileID.ToString() + DataFileExtension))
          io = IOManagerFactory.Create(_options.IOType,Path.Join(_options.DirPath,activeDataFile.FileID.ToString() + DataFileExtension));
        var newActiveDataFile = new DataFile(activeDataFile.FileID + 1,io);
        _activeFile = newActiveDataFile;
        
        dataFiles.Add(activeDataFile);

        var mergeDir = GetMergeDirPath();

        if (Directory.Exists(mergeDir))
        {
            Directory.Delete(mergeDir,true);
        }

        Directory.CreateDirectory(mergeDir);

        var option = new EngineOptions(mergeDir,_options.DataFileSize);
        var mergeEngine = new Engine(option);

        // 创建Hint文件，用于记录重写时的索引
        var hintFile = CreateHintFile(mergeDir);
        foreach (var df in dataFiles)
        {
            UInt64 offset = 0;
            // 循环加载数据文件中的所有记录
            while (true)
            {
                var logRecord = df.ReadLogRecord(offset);
                if (logRecord == null)
                {
                    // 读到最后一条数据了
                    break;
                }
                
                var (_, key) = ParseTransactionNumberAndKey(logRecord.Key);
                // 检查是否是有效数据 
                // fileID & key & offset 跟内存索引的数据相等 数据才有效
                if (Index.Contains(key))
                {
                   LogRecordPos pos = Index.Get(key)!;
                   if (pos.FileID == df.FileID && offset == pos.Offset)
                   {
                       // 重写到用于合并的存储引擎实例
                       // 建事务序列号修改为普通数据
                       logRecord.Key = EncodeKeyWithTransactionNo(key, NormalNumber);
                       var newPos= mergeEngine.AppendLogRecord(logRecord);
                       
                       // 记录重写后的索引
                       var hintRecord = new LogRecord()
                       {
                            Key = key,
                            Value = LogRecordPos.Encode(newPos),
                            RecordType = LogRecordType.Normal
                       };
                       hintFile.Write(hintRecord.Encode());

                       reducedReclaimableSize += (UInt32)logRecord.Encode().Length;
                   }
                }
                offset += (UInt64)(LogRecord.MaxHeaderSize() + logRecord.Key.Length + logRecord.Value.Length + 4);
            }
        }
        
        // 保证数据持久化
        mergeEngine.Sync();
        hintFile.Sync();
        
        // 创建一个文件标识Merge完成
        var finishedFile = CreateMergeFinishedFile(mergeDir);
        // 记录最近没有参与合并的数据文件ID
        var finishedRecord = new LogRecord()
        {
            Key = Encoding.UTF8.GetBytes(MergeFinishedFileName),
            Value = BitConverter.GetBytes(activeDataFile.FileID + 1),
            RecordType = LogRecordType.Normal,
        };
        finishedFile.Write(finishedRecord.Encode());
        
        finishedFile.Sync();

        _reclaimableSize -= reducedReclaimableSize;
    }

    private string GetMergeDirPath()
    {
        // 跟原引擎文件夹同一目录
        return _options.DirPath + MergeDirName;
    }

    private DataFile CreateHintFile(string dir)
    {
        //var io = new FileIO(Path.Join(dir,HintFileName));
        var io = IOManagerFactory.Create(_options.IOType,Path.Join(dir,HintFileName));
        return new DataFile(0,io);
    }
    private DataFile CreateMergeFinishedFile(string dir)
    {
       //  var io = new FileIO(Path.Join(dir,MergeFinishedFileName));
       var io = IOManagerFactory.Create(_options.IOType,Path.Join(dir,MergeFinishedFileName));
        return new DataFile(0,io);
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

    public Stat GetStat() 
        => new Stat(Index.GetKeys().Count(),
            _olderFiles.Count + 1,
            _reclaimableSize,
            (UInt64)DBHelper.GetDirectorySize(_options.DirPath));
    
    
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
    
    
    /// <summary>
    /// 备份当前数据库目录到 destDir
    /// </summary>
    /// <param name="destDir">目标目录</param>
    public void BackUp(string destDir)
    {
        if (!Directory.Exists(destDir))
        {
            Directory.CreateDirectory(destDir);
        }
       DBHelper.CopyDirectory(_options.DirPath,destDir); 
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