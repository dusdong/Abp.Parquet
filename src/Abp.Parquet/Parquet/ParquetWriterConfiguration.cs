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
    /// 异常重试次数，默认为 3
    /// </summary>
    public int RetryCount { get; set; } = 3;
    
    /// <summary>
    /// 重试等待时间（毫秒），默认为 1000ms
    /// </summary>
    public int RetryDelayMilliseconds { get; set; } = 1000;
    
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
            BatchSize = 50000                          // 更大的批次
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
            BatchSize = 10000                          // 适中的批次大小
        };
    }
}
