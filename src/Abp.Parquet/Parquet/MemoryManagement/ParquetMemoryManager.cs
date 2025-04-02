using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Abp.Parquet.MemoryManagement;

/// <summary>
/// Parquet 内存管理器，用于监控和优化内存使用
/// </summary>
public sealed class ParquetMemoryManager
{
    private static readonly Lazy<ParquetMemoryManager> _instance = new(() => new ParquetMemoryManager());
    
    /// <summary>
    /// 全局实例
    /// </summary>
    public static ParquetMemoryManager Instance => _instance.Value;
    
    /// <summary>
    /// 内存使用追踪器字典
    /// </summary>
    private readonly ConcurrentDictionary<string, MemoryTracker> _memoryTrackers = new();
    
    /// <summary>
    /// 全局内存限制（字节）
    /// </summary>
    public long MemoryLimit { get; set; } = 1L * 1024 * 1024 * 1024; // 默认 1GB
    
    /// <summary>
    /// 当前已分配内存（字节）
    /// </summary>
    public long AllocatedMemory { get; private set; }
    
    /// <summary>
    /// 是否启用内存限制
    /// </summary>
    public bool EnableMemoryLimit { get; set; } = false;
    
    /// <summary>
    /// 是否启用内存使用日志
    /// </summary>
    public bool EnableMemoryLogging { get; set; } = false;
    
    /// <summary>
    /// 获取指定 ID 的内存追踪器
    /// </summary>
    /// <param name="trackerId">追踪器 ID</param>
    /// <returns>内存追踪器</returns>
    public MemoryTracker GetTracker(string trackerId)
    {
        return _memoryTrackers.GetOrAdd(trackerId, id => new MemoryTracker(id, this));
    }
    
    /// <summary>
    /// 尝试分配内存
    /// </summary>
    /// <param name="bytes">字节数</param>
    /// <param name="trackerId">追踪器 ID</param>
    /// <returns>是否成功分配</returns>
    public bool TryAllocate(long bytes, string trackerId)
    {
        if (!EnableMemoryLimit) return true;
        
        lock (this)
        {
            if (AllocatedMemory + bytes <= MemoryLimit)
            {
                AllocatedMemory += bytes;
                
                if (EnableMemoryLogging)
                {
                    Console.WriteLine($"[ParquetMemory] 分配内存: {FormatBytes(bytes)}, 追踪器: {trackerId}, 当前总内存: {FormatBytes(AllocatedMemory)}");
                }
                
                return true;
            }
            
            if (EnableMemoryLogging)
            {
                Console.WriteLine($"[ParquetMemory] 内存分配失败: 请求 {FormatBytes(bytes)}, 可用 {FormatBytes(MemoryLimit - AllocatedMemory)}, 追踪器: {trackerId}");
            }
            
            return false;
        }
    }
    
    /// <summary>
    /// 释放内存
    /// </summary>
    /// <param name="bytes">字节数</param>
    /// <param name="trackerId">追踪器 ID</param>
    public void Release(long bytes, string trackerId)
    {
        if (!EnableMemoryLimit) return;
        
        lock (this)
        {
            AllocatedMemory = Math.Max(0, AllocatedMemory - bytes);
            
            if (EnableMemoryLogging)
            {
                Console.WriteLine($"[ParquetMemory] 释放内存: {FormatBytes(bytes)}, 追踪器: {trackerId}, 当前总内存: {FormatBytes(AllocatedMemory)}");
            }
        }
    }
    
    /// <summary>
    /// 获取内存使用情况
    /// </summary>
    public MemoryUsageInfo GetMemoryUsageInfo()
    {
        var trackers = new List<TrackerInfo>();
        
        foreach (var tracker in _memoryTrackers.Values)
        {
            trackers.Add(new TrackerInfo
            {
                Id = tracker.Id,
                CurrentAllocation = tracker.CurrentAllocation,
                PeakAllocation = tracker.PeakAllocation,
                TotalAllocations = tracker.TotalAllocations,
                TotalDeallocations = tracker.TotalDeallocations
            });
        }
        
        return new MemoryUsageInfo
        {
            TotalAllocated = AllocatedMemory,
            MemoryLimit = MemoryLimit,
            Trackers = trackers
        };
    }
    
    /// <summary>
    /// 重置所有内存追踪器
    /// </summary>
    public void Reset()
    {
        lock (this)
        {
            AllocatedMemory = 0;
            foreach (var tracker in _memoryTrackers.Values)
            {
                tracker.Reset();
            }
        }
    }
    
    /// <summary>
    /// 移除指定 ID 的内存追踪器
    /// </summary>
    /// <param name="trackerId">追踪器 ID</param>
    public void RemoveTracker(string trackerId)
    {
        if (_memoryTrackers.TryRemove(trackerId, out var tracker))
        {
            Release(tracker.CurrentAllocation, trackerId);
        }
    }
    
    /// <summary>
    /// 从对象池租用指定类型和大小的数组
    /// </summary>
    /// <typeparam name="T">数组元素类型</typeparam>
    /// <param name="minimumLength">最小长度</param>
    /// <param name="trackerId">追踪器 ID</param>
    /// <returns>数组租用对象</returns>
    public ArrayPoolRental<T> RentArray<T>(int minimumLength, string trackerId)
    {
        long byteSize = (long)minimumLength * Unsafe.SizeOf<T>();
        
        // 检查内存限制
        if (EnableMemoryLimit && !TryAllocate(byteSize, trackerId))
        {
            throw new ParquetMemoryLimitException(
                $"无法分配内存: 请求 {FormatBytes(byteSize)}, 可用 {FormatBytes(MemoryLimit - AllocatedMemory)}", 
                byteSize, 
                MemoryLimit - AllocatedMemory);
        }
        
        // 从池中租用数组
        T[] array = ArrayPool<T>.Shared.Rent(minimumLength);
        
        return new ArrayPoolRental<T>(array, trackerId, this, byteSize);
    }
    
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
/// 内存追踪器，用于跟踪特定组件或操作的内存使用
/// </summary>
public class MemoryTracker
{
    private readonly ParquetMemoryManager _manager;
    
    /// <summary>
    /// 追踪器 ID
    /// </summary>
    public string Id { get; }
    
    /// <summary>
    /// 当前分配的内存（字节）
    /// </summary>
    public long CurrentAllocation { get; private set; }
    
    /// <summary>
    /// 峰值内存分配（字节）
    /// </summary>
    public long PeakAllocation { get; private set; }
    
    /// <summary>
    /// 总分配次数
    /// </summary>
    public long TotalAllocations { get; private set; }
    
    /// <summary>
    /// 总释放次数
    /// </summary>
    public long TotalDeallocations { get; private set; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="id">追踪器 ID</param>
    /// <param name="manager">内存管理器</param>
    internal MemoryTracker(string id, ParquetMemoryManager manager)
    {
        Id = id;
        _manager = manager;
    }
    
    /// <summary>
    /// 尝试分配内存
    /// </summary>
    /// <param name="bytes">字节数</param>
    /// <returns>是否成功分配</returns>
    public bool TryAllocate(long bytes)
    {
        if (_manager.TryAllocate(bytes, Id))
        {
            lock (this)
            {
                CurrentAllocation += bytes;
                PeakAllocation = Math.Max(PeakAllocation, CurrentAllocation);
                TotalAllocations++;
            }
            return true;
        }
        return false;
    }
    
    /// <summary>
    /// 释放内存
    /// </summary>
    /// <param name="bytes">字节数</param>
    public void Release(long bytes)
    {
        _manager.Release(bytes, Id);
        
        lock (this)
        {
            CurrentAllocation = Math.Max(0, CurrentAllocation - bytes);
            TotalDeallocations++;
        }
    }
    
    /// <summary>
    /// 重置追踪器状态
    /// </summary>
    public void Reset()
    {
        lock (this)
        {
            _manager.Release(CurrentAllocation, Id);
            CurrentAllocation = 0;
            TotalAllocations = 0;
            TotalDeallocations = 0;
            // 保留峰值记录，方便分析
        }
    }

    /// <summary>
    /// 从对象池租用指定类型和大小的数组
    /// </summary>
    /// <typeparam name="T">数组元素类型</typeparam>
    /// <param name="minimumLength">最小长度</param>
    /// <returns>数组租用对象</returns>
    public ArrayPoolRental<T> RentArray<T>(int minimumLength)
    {
        return _manager.RentArray<T>(minimumLength, Id);
    }
}

/// <summary>
/// 内存使用统计信息
/// </summary>
public class MemoryUsageInfo
{
    /// <summary>
    /// 总分配内存（字节）
    /// </summary>
    public long TotalAllocated { get; set; }
    
    /// <summary>
    /// 内存限制（字节）
    /// </summary>
    public long MemoryLimit { get; set; }
    
    /// <summary>
    /// 追踪器信息列表
    /// </summary>
    public List<TrackerInfo> Trackers { get; set; } = new List<TrackerInfo>();
}

/// <summary>
/// 追踪器信息
/// </summary>
public class TrackerInfo
{
    /// <summary>
    /// 追踪器 ID
    /// </summary>
    public string Id { get; set; }
    
    /// <summary>
    /// 当前分配的内存（字节）
    /// </summary>
    public long CurrentAllocation { get; set; }
    
    /// <summary>
    /// 峰值内存分配（字节）
    /// </summary>
    public long PeakAllocation { get; set; }
    
    /// <summary>
    /// 总分配次数
    /// </summary>
    public long TotalAllocations { get; set; }
    
    /// <summary>
    /// 总释放次数
    /// </summary>
    public long TotalDeallocations { get; set; }
}

/// <summary>
/// 从ArrayPool租用的数组，实现IDisposable自动归还
/// </summary>
/// <typeparam name="T">数组元素类型</typeparam>
public readonly struct ArrayPoolRental<T> : IDisposable
{
    private readonly T[] _array;
    private readonly string _trackerId;
    private readonly ParquetMemoryManager _manager;
    private readonly long _byteSize;
    
    /// <summary>
    /// 获取租用的数组
    /// </summary>
    public ReadOnlySpan<T> Span => _array.AsSpan();
    
    /// <summary>
    /// 获取租用的数组作为Memory
    /// </summary>
    public Memory<T> Memory => _array.AsMemory();
    
    /// <summary>
    /// 获取底层数组
    /// </summary>
    public T[] Array => _array;
    
    /// <summary>
    /// 构造函数
    /// </summary>
    internal ArrayPoolRental(T[] array, string trackerId, ParquetMemoryManager manager, long byteSize)
    {
        _array = array;
        _trackerId = trackerId;
        _manager = manager;
        _byteSize = byteSize;
    }
    
    /// <summary>
    /// 释放并归还数组到池
    /// </summary>
    public void Dispose()
    {
        if (_array != null)
        {
            // 清除可能包含敏感数据的数组
            ArrayPool<T>.Shared.Return(_array, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            _manager.Release(_byteSize, _trackerId);
        }
    }
}
