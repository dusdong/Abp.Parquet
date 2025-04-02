using System.Buffers;
using System.Runtime.CompilerServices;

namespace Abp.Parquet.MemoryManagement;

/// <summary>
/// 内存统计信息，用于监控内存使用情况
/// </summary>
public class MemoryStatistics
{
    private long _totalAllocated;
    private long _currentAllocated;
    private long _peakAllocated;
    private int _allocationCount;
    private int _releaseCount;
    private readonly object _lock = new object();
    
    /// <summary>
    /// 总共分配的内存（字节）
    /// </summary>
    public long TotalAllocated => _totalAllocated;
    
    /// <summary>
    /// 当前分配的内存（字节）
    /// </summary>
    public long CurrentAllocated => _currentAllocated;
    
    /// <summary>
    /// 峰值内存使用（字节）
    /// </summary>
    public long PeakAllocated => _peakAllocated;
    
    /// <summary>
    /// 内存分配次数
    /// </summary>
    public int AllocationCount => _allocationCount;
    
    /// <summary>
    /// 内存释放次数
    /// </summary>
    public int ReleaseCount => _releaseCount;
    
    /// <summary>
    /// 记录内存分配
    /// </summary>
    /// <param name="bytes">分配的字节数</param>
    public void RecordAllocation(long bytes)
    {
        if (bytes <= 0) return;
        
        lock (_lock)
        {
            _totalAllocated += bytes;
            _currentAllocated += bytes;
            _peakAllocated = Math.Max(_peakAllocated, _currentAllocated);
            _allocationCount++;
        }
    }
    
    /// <summary>
    /// 记录内存释放
    /// </summary>
    /// <param name="bytes">释放的字节数</param>
    public void RecordRelease(long bytes)
    {
        if (bytes <= 0) return;
        
        lock (_lock)
        {
            _currentAllocated = Math.Max(0, _currentAllocated - bytes);
            _releaseCount++;
        }
    }
    
    /// <summary>
    /// 重置统计信息
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _totalAllocated = 0;
            _currentAllocated = 0;
            // 保留峰值记录
            _allocationCount = 0;
            _releaseCount = 0;
        }
    }
    
    /// <summary>
    /// 获取统计信息的字符串表示
    /// </summary>
    /// <returns>统计信息字符串</returns>
    public override string ToString()
    {
        return $"Memory Stats: Current={FormatBytes(_currentAllocated)}, " +
               $"Peak={FormatBytes(_peakAllocated)}, " +
               $"Total={FormatBytes(_totalAllocated)}, " +
               $"Allocs={_allocationCount}, Releases={_releaseCount}";
    }
    
    /// <summary>
    /// 格式化字节数为人类可读形式
    /// </summary>
    private static string FormatBytes(long bytes)
    {
        string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
        int counter = 0;
        double number = bytes;
        while (number > 1024 && counter < suffixes.Length - 1)
        {
            number /= 1024;
            counter++;
        }
        return $"{number:0.##} {suffixes[counter]}";
    }
}

/// <summary>
/// 高效内存缓冲区管理器
/// </summary>
public class BufferManager : IDisposable
{
    private readonly ArrayPool<byte> _bytePool;
    private readonly MemoryStatistics _stats = new();
    private bool _isDisposed;
    private readonly bool _trackMemory;
    private readonly bool _clearBuffers;
    
    /// <summary>
    /// 获取内存统计信息
    /// </summary>
    public MemoryStatistics Statistics => _stats;
    
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="trackMemory">是否跟踪内存使用</param>
    /// <param name="clearBuffers">是否清除返回的缓冲区</param>
    public BufferManager(bool trackMemory = true, bool clearBuffers = false)
    {
        _bytePool = ArrayPool<byte>.Create();
        _trackMemory = trackMemory;
        _clearBuffers = clearBuffers;
    }
    
    /// <summary>
    /// 分配指定大小的字节数组
    /// </summary>
    /// <param name="minBytes">最小字节数</param>
    /// <returns>字节数组</returns>
    public ArraySegment<byte> Rent(int minBytes)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(BufferManager));
            
        if (minBytes <= 0)
            return new ArraySegment<byte>(Array.Empty<byte>());
            
        byte[] buffer = _bytePool.Rent(minBytes);
        
        if (_trackMemory)
        {
            _stats.RecordAllocation(buffer.Length);
        }
        
        return new ArraySegment<byte>(buffer, 0, minBytes);
    }
    
    /// <summary>
    /// 返回字节数组到池中
    /// </summary>
    /// <param name="buffer">字节数组</param>
    public void Return(byte[] buffer)
    {
        if (_isDisposed || buffer == null || buffer.Length == 0)
            return;
            
        _bytePool.Return(buffer, _clearBuffers);
        
        if (_trackMemory)
        {
            _stats.RecordRelease(buffer.Length);
        }
    }
    
    /// <summary>
    /// 返回字节数组段到池中
    /// </summary>
    /// <param name="segment">字节数组段</param>
    public void Return(ArraySegment<byte> segment)
    {
        Return(segment.Array);
    }
    
    /// <summary>
    /// 获取可自动返回的缓冲区
    /// </summary>
    /// <param name="minBytes">最小字节数</param>
    /// <returns>可释放缓冲区</returns>
    public DisposableBuffer RentDisposable(int minBytes)
    {
        var segment = Rent(minBytes);
        return new DisposableBuffer(this, segment);
    }
    
    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed)
            return;
            
        _isDisposed = true;
        GC.SuppressFinalize(this);
    }
    
    /// <summary>
    /// 可自动释放的缓冲区
    /// </summary>
    public readonly struct DisposableBuffer : IDisposable
    {
        private readonly BufferManager _manager;
        private readonly ArraySegment<byte> _buffer;
        
        /// <summary>
        /// 底层缓冲区段
        /// </summary>
        public ArraySegment<byte> Buffer => _buffer;
        
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="manager">缓冲区管理器</param>
        /// <param name="buffer">缓冲区段</param>
        internal DisposableBuffer(BufferManager manager, ArraySegment<byte> buffer)
        {
            _manager = manager;
            _buffer = buffer;
        }
        
        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            _manager.Return(_buffer);
        }
    }
}

/// <summary>
/// 泛型数组租用，支持自动返回到池
/// </summary>
/// <typeparam name="T">数组元素类型</typeparam>
public readonly struct ArrayRental<T> : IDisposable
{
    private readonly ArrayPool<T> _pool;
    private readonly T[] _array;
    private readonly bool _clearOnReturn;
    
    /// <summary>
    /// 获取租用的数组
    /// </summary>
    public T[] Array => _array;
    
    /// <summary>
    /// 获取数组的内存段
    /// </summary>
    public Memory<T> Memory => _array;
    
    /// <summary>
    /// 获取数组的只读跨度
    /// </summary>
    public ReadOnlySpan<T> Span => _array;
    
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="minLength">最小长度</param>
    /// <param name="clearOnReturn">返回时是否清除</param>
    public ArrayRental(int minLength, bool clearOnReturn = false)
    {
        _pool = ArrayPool<T>.Shared;
        _array = _pool.Rent(minLength);
        _clearOnReturn = clearOnReturn || RuntimeHelpers.IsReferenceOrContainsReferences<T>();
    }
    
    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_array != null)
        {
            _pool.Return(_array, _clearOnReturn);
        }
    }
}
