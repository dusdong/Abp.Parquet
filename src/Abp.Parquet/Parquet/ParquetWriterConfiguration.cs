using System.IO.Compression;
using Parquet;

namespace Abp.Parquet;

/// <summary>
/// Parquet 文件写入器配置
/// </summary>
public class ParquetWriterConfiguration
{
    /// <summary>
    /// 默认批处理大小
    /// </summary>
    public static readonly int DefaultBatchSize = 10000;
    
    /// <summary>
    /// 默认缓冲区大小
    /// </summary>
    public static readonly int DefaultBufferSize = 8192;
    
    /// <summary>
    /// Parquet 文件写入选项
    /// </summary>
    public ParquetOptions ParquetOptions { get; set; } = new ParquetOptions();

    /// <summary>
    /// 压缩方法，默认使用Gzip压缩
    /// </summary>
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Gzip;

    /// <summary>
    /// 压缩级别（部分压缩方法支持），默认使用最佳压缩
    /// </summary>
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;
    
    /// <summary>
    /// 批处理大小，当记录数超过此值时进行分批写入
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;
    
    /// <summary>
    /// 文件读写缓冲区大小（字节）
    /// </summary>
    public int BufferSize { get; set; } = DefaultBufferSize;
    
    /// <summary>
    /// 是否使用异步IO
    /// </summary>
    public bool UseAsyncIO { get; set; } = true;
    
    /// <summary>
    /// 是否使用顺序访问优化
    /// </summary>
    public bool UseSequentialScan { get; set; } = true;

    /// <summary>
    /// 自定义元数据
    /// </summary>
    public Dictionary<string, string> CustomMetadata { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// 忽略的字段集合
    /// </summary>
    public HashSet<string> IgnoredFields { get; set; } = new HashSet<string>();

    /// <summary>
    /// 自定义类型映射函数，用于将 .NET 类型映射到 Parquet 类型
    /// </summary>
    public Func<Type, Type> MapParquetType { get; set; }
    
    /// <summary>
    /// 需要特殊处理的字段与其处理方式
    /// </summary>
    public Dictionary<string, FieldProcessingConfig> FieldProcessingMap { get; set; } = new Dictionary<string, FieldProcessingConfig>();

    /// <summary>
    /// 是否将 DateTime 视为 DateTimeOffset
    /// </summary>
    public bool TreatDateTimeAsDateTimeOffset { get; set; } = false;

    /// <summary>
    /// 是否将 DateTime 视为字符串
    /// </summary>
    public bool TreatDateTimeAsString { get; set; } = false;

    /// <summary>
    /// 是否将 DateTimeOffset 视为字符串
    /// </summary>
    public bool TreatDateTimeOffsetAsString { get; set; } = false;
    
    /// <summary>
    /// 异常重试次数
    /// </summary>
    public int RetryCount { get; set; } = 3;
    
    /// <summary>
    /// 重试等待时间（毫秒）
    /// </summary>
    public int RetryDelayMs { get; set; } = 200;
    
    /// <summary>
    /// 是否启用内存管理
    /// </summary>
    public bool EnableMemoryManagement { get; set; } = true;
    
    /// <summary>
    /// 是否使用数组池
    /// </summary>
    public bool UseArrayPool { get; set; } = true;
    
    /// <summary>
    /// 是否清除使用后的数组（可能会降低性能，但增加安全性）
    /// </summary>
    public bool ClearArraysAfterUse { get; set; } = false;
    
    /// <summary>
    /// 是否启用内存使用限制
    /// </summary>
    public bool EnableMemoryLimit { get; set; } = false;
    
    /// <summary>
    /// 内存使用限制（字节）
    /// </summary>
    public long MemoryLimit { get; set; } = 1L * 1024 * 1024 * 1024; // 默认 1GB
    
    /// <summary>
    /// 最小记录数进行并行处理
    /// </summary>
    public int MinimumRecordsForParallel { get; set; } = 5000;
    
    /// <summary>
    /// 最大并行度，默认与处理器数量相同
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    
    /// <summary>
    /// 是否启用并行处理
    /// </summary>
    public bool EnableParallelProcessing { get; set; } = false;
    
    /// <summary>
    /// 是否在内存不足时自动调整并行度
    /// </summary>
    public bool AutoAdjustParallelism { get; set; } = true;
    
    /// <summary>
    /// 是否启用反射缓存
    /// </summary>
    public bool EnableReflectionCache { get; set; } = true;
    
    /// <summary>
    /// 是否启用自动类型转换
    /// </summary>
    public bool EnableAutomaticTypeConversion { get; set; } = true;
    
    /// <summary>
    /// 自定义类型转换器
    /// </summary>
    public Dictionary<(Type sourceType, Type targetType), Func<Array, Array>> CustomTypeConverters { get; set; } = 
        new Dictionary<(Type sourceType, Type targetType), Func<Array, Array>>();
        
    /// <summary>
    /// 向Parquet类型转换注册表注册自定义转换器
    /// </summary>
    public void RegisterCustomTypeConverters()
    {
        var registry = ParquetTypeConverterRegistry.Instance;
        foreach (var item in CustomTypeConverters)
        {
            registry.Register(item.Key.sourceType, item.Key.targetType, item.Value);
        }
    }
    
    /// <summary>
    /// 添加自定义类型转换器
    /// </summary>
    /// <typeparam name="TSource">源类型</typeparam>
    /// <typeparam name="TTarget">目标类型</typeparam>
    /// <param name="converter">转换函数</param>
    public void AddTypeConverter<TSource, TTarget>(Func<TSource[], TTarget[]> converter)
    {
        CustomTypeConverters.Add((typeof(TSource), typeof(TTarget)), 
            array => converter((TSource[])array));
    }
    
    /// <summary>
    /// 为大数据集创建优化的配置
    /// </summary>
    /// <returns>针对大数据集优化的配置</returns>
    public static ParquetWriterConfiguration CreateForLargeDatasets()
    {
        return new ParquetWriterConfiguration
        {
            CompressionMethod = CompressionMethod.Gzip,
            CompressionLevel = CompressionLevel.Fastest, // 优先速度
            BatchSize = 50000,                         // 更大的批次
            BufferSize = 32768,                        // 更大的缓冲区
            UseAsyncIO = true,
            UseSequentialScan = true,
            EnableMemoryManagement = true,
            UseArrayPool = true,
            ClearArraysAfterUse = false,               // 关闭清理以提高性能
            EnableParallelProcessing = true,           // 启用并行处理
            MaxDegreeOfParallelism = Environment.ProcessorCount - 1, // 留一个核心给系统
            RetryCount = 5,                            // 更多重试次数
            EnableAutomaticTypeConversion = true       // 启用自动类型转换
        };
    }
    
    /// <summary>
    /// 为高压缩率创建优化的配置
    /// </summary>
    /// <returns>针对高压缩率优化的配置</returns>
    public static ParquetWriterConfiguration CreateForMaximumCompression()
    {
        return new ParquetWriterConfiguration
        {
            CompressionMethod = CompressionMethod.Gzip,
            CompressionLevel = CompressionLevel.Optimal, // 优先压缩率
            BatchSize = 10000,                         // 适中的批次大小
            BufferSize = 16384,                        // 中等缓冲区
            UseAsyncIO = true,
            UseSequentialScan = true,
            EnableMemoryManagement = true,
            UseArrayPool = true,
            ClearArraysAfterUse = false,
            EnableParallelProcessing = false,          // 关闭并行处理提高压缩率
            EnableAutomaticTypeConversion = true       // 启用自动类型转换
        };
    }

    /// <summary>
    /// 为内存受限环境创建优化的配置
    /// </summary>
    /// <returns>针对内存受限环境优化的配置</returns>
    public static ParquetWriterConfiguration CreateForMemoryConstrainedEnvironment()
    {
        return new ParquetWriterConfiguration
        {
            CompressionMethod = CompressionMethod.Gzip,
            CompressionLevel = CompressionLevel.Optimal,
            BatchSize = 5000,                          // 小批次减少内存使用
            BufferSize = 4096,                         // 小缓冲区
            UseAsyncIO = true,
            UseSequentialScan = true,
            EnableMemoryManagement = true,
            UseArrayPool = true,                       // 使用池减少分配
            ClearArraysAfterUse = true,                // 立即清除释放内存
            EnableMemoryLimit = true,                  // 启用内存限制
            MemoryLimit = 200 * 1024 * 1024,           // 设置较低的内存限制 (200MB)
            EnableParallelProcessing = false,          // 关闭并行处理减少内存消耗
            RetryCount = 3,
            RetryDelayMs = 500,                        // 更长的重试等待时间
            EnableAutomaticTypeConversion = true       // 启用自动类型转换
        };
    }
    
    /// <summary>
    /// 为高吞吐量创建优化的配置
    /// </summary>
    /// <returns>针对高吞吐量优化的配置</returns>
    public static ParquetWriterConfiguration CreateForHighThroughput()
    {
        // 获取系统处理器数量，但最多使用16个
        int processorCount = Math.Min(Environment.ProcessorCount, 16);
        
        return new ParquetWriterConfiguration
        {
            CompressionMethod = CompressionMethod.Snappy, // 使用更快的压缩算法
            BatchSize = 25000,                          // 中等批次
            BufferSize = 65536,                         // 大缓冲区
            UseAsyncIO = true,
            UseSequentialScan = true,
            EnableMemoryManagement = true,
            UseArrayPool = true,
            ClearArraysAfterUse = false,                // 关闭清理以提高性能
            EnableParallelProcessing = true,            // 启用并行处理
            MaxDegreeOfParallelism = processorCount,    // 充分利用处理器
            AutoAdjustParallelism = true,               // 自动调整并行度
            MinimumRecordsForParallel = 2000,           // 更小的并行阈值
            RetryCount = 2,                             // 更少的重试以避免延迟
            EnableAutomaticTypeConversion = true        // 启用自动类型转换
        };
    }

    /// <summary>
    /// 添加字段特殊处理配置
    /// </summary>
    /// <param name="fieldName">字段名称</param>
    /// <param name="config">处理配置</param>
    public void AddFieldProcessing(string fieldName, FieldProcessingConfig config)
    {
        FieldProcessingMap[fieldName] = config;
    }

    /// <summary>
    /// 添加 DateTime 到 DateTime? 的特殊转换
    /// </summary>
    /// <param name="fieldName">字段名称</param>
    public void AddDateTimeToNullableDateTimeConversion(string fieldName)
    {
        FieldProcessingMap[fieldName] = new FieldProcessingConfig 
        {
            SourceType = typeof(DateTime),
            TargetType = typeof(DateTime?),
            ProcessingType = FieldProcessingType.SpecialDateTimeConversion
        };
    }
}

/// <summary>
/// 字段处理配置
/// </summary>
public class FieldProcessingConfig
{
    /// <summary>
    /// 源数据类型
    /// </summary>
    public Type SourceType { get; set; }
    
    /// <summary>
    /// 目标数据类型
    /// </summary>
    public Type TargetType { get; set; }
    
    /// <summary>
    /// 处理类型
    /// </summary>
    public FieldProcessingType ProcessingType { get; set; }
    
    /// <summary>
    /// 自定义转换函数
    /// </summary>
    public Func<object, object> Converter { get; set; }
}

/// <summary>
/// 字段处理类型
/// </summary>
public enum FieldProcessingType
{
    /// <summary>
    /// 默认处理
    /// </summary>
    Default,
    
    /// <summary>
    /// 特殊的DateTime转换
    /// </summary>
    SpecialDateTimeConversion,
    
    /// <summary>
    /// 自定义转换
    /// </summary>
    CustomConverter
}
