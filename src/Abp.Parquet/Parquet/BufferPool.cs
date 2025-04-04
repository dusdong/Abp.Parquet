using System.Collections.Concurrent;

namespace Abp.Parquet;

/// <summary>
/// 缓冲区池，用于优化内存使用
/// </summary>
internal class BufferPool
{
    private readonly ConcurrentBag<byte[]> _buffers = new ConcurrentBag<byte[]>();
    private readonly int _bufferSize;
    private int _totalBuffers;
    private readonly int _maxBuffers;
        
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="bufferSize">每个缓冲区的大小（字节）</param>
    /// <param name="maxBuffers">池中最大缓冲区数量</param>
    public BufferPool(int bufferSize = 81920, int maxBuffers = 50)
    {
        _bufferSize = bufferSize;
        _maxBuffers = maxBuffers;
    }
        
    /// <summary>
    /// 租用一个缓冲区
    /// </summary>
    public byte[] Rent()
    {
        if (_buffers.TryTake(out var buffer))
        {
            return buffer;
        }
            
        // 创建新的缓冲区
        buffer = new byte[_bufferSize];
        Interlocked.Increment(ref _totalBuffers);
        return buffer;
    }
        
    /// <summary>
    /// 归还一个缓冲区到池中
    /// </summary>
    public void Return(byte[] buffer)
    {
        if (buffer == null || buffer.Length != _bufferSize)
        {
            return; // 忽略无效缓冲区或大小不匹配的缓冲区
        }
            
        // 如果不超过最大缓冲区数量，则保留在池中
        if (_totalBuffers <= _maxBuffers)
        {
            _buffers.Add(buffer);
        }
    }
        
    /// <summary>
    /// 清空缓冲区池
    /// </summary>
    public void Clear()
    {
        while (_buffers.TryTake(out _)) { }
        _totalBuffers = 0;
    }
}
