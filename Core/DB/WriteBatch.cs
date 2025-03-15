using CommunityToolkit.Diagnostics;
using Core.Data;

namespace Core.DB;

public class WriteBatch
{
    private Dictionary<byte[], LogRecord> _pendingWrites = new();
    private readonly Engine _engine;
    private readonly WriteBatchOptions _options;

    public WriteBatch(Engine engine, WriteBatchOptions options)
    {
        _engine = engine;
        _options = options;
    }
    
    public void Put(byte[] key,byte[] value)
    {
        Guard.IsNotNull(key);
        Guard.IsNotNull(value);
        
        // 将事务序列号添加到Key中
        var record = new LogRecord()
        {
            Key = key,
            Value = value,
            RecordType = LogRecordType.Normal,
        };
        
        _pendingWrites.Add(key,record);        
    }

    public void Delete(byte[] key)
    {
        Guard.IsNotNull(key);
        
        // 判断内存索引中是否存在
        if (!_engine.Contains(key))
        {
            return;
        }

        if (_pendingWrites.ContainsKey(key))
        {
            _pendingWrites.Remove(key);
            return;
        } 
        
        var record = new LogRecord()
        {
            Key = key,
            Value = [],
            RecordType = LogRecordType.Deleted,
        };
        
        _pendingWrites.Add(key, record);
    }

    public void Commit()
    {
        if (_pendingWrites.Count() < 0)
        {
            return;
        }

        if (_pendingWrites.Count() > _options.MaxBatchSize)
        {
            ThrowHelper.ThrowInvalidOperationException("batch size exceeded");
        }
        // 加锁保证事务提交串行化
        _engine.CommitLock.WaitOne();
        
        // 获取全局事务序列号
        var transactionNo = _engine.IncrementTransactionNumber();
        
        var positions = new Dictionary<byte[], LogRecordPos>();
        foreach (var (_,item) in _pendingWrites)
        {
            var record = new LogRecord()
            {
                Key = _engine.EncodeKeyWithTransactionNo(item.Key, transactionNo),
                Value = item.Value,
                RecordType = item.RecordType,
            };
            var pos = _engine.AppendLogRecord(record);
            positions.Add(item.Key,pos);
        }

        var finishRecord = new LogRecord()
        {
            Key = _engine.EncodeKeyWithTransactionNo([], transactionNo),
            Value = [],
            RecordType = LogRecordType.TransactionFinished,
        };
        _engine.AppendLogRecord(finishRecord);

        if (_options.SyncWhenCommited)
        {
            _engine.Sync();
        }
        
        // 数据全部写完之后更新内存索引

        foreach (var (key,record) in _pendingWrites)
        {
            if (record.RecordType == LogRecordType.Normal)
            {
                _engine.Index.Put(key, positions[key]);
            }

            if (record.RecordType == LogRecordType.Deleted)
            {
                _engine.Index.Delete(key);
            }
        }
        _pendingWrites.Clear();
    }
    
   
    
}