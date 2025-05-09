﻿namespace Core.DB;

public class WriteBatchOptions
{
    // 单批次同时写入的最大数量
    public UInt32 MaxBatchSize { get; set; }
    
    // 提交后是否立即同步数据持久化
    public bool SyncWhenCommited { get; set; }
}