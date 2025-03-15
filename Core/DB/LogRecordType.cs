namespace Core.DB;

internal enum LogRecordType
{
    Normal = 1,
    Deleted ,
    TransactionFinished, // 事务
}