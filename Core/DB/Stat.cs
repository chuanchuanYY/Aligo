namespace Core.DB;


// Statistics System Data
public class Stat
{
    public int KeyNum { get; set; }
    public int DataFileNum { get; set; }
    public UInt64 ReclaimableSize { get; set; }
    public UInt64  DiskSize { get; set; }

    public Stat(int keyNum, int dataFileNum, UInt64 reclaimableSize, UInt64 diskSize)
    {
        this.KeyNum = keyNum;
        this.DataFileNum = dataFileNum;
        this.ReclaimableSize = reclaimableSize;
        this.DiskSize = diskSize;
    }

    public override string ToString()
    {
        return $"keyNum:{KeyNum}, dataFileNum:{DataFileNum}, reclaimableSize:{ReclaimableSize}, diskSize:{DiskSize}";
    }
}