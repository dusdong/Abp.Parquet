using System.IO.Compression;
using Parquet;

namespace Abp.Parquet;

/// <summary>
/// Parquet 文件写入器配置，替代 ChoParquetRecordConfiguration
/// </summary>
public class ParquetWriterConfiguration
{
    /// <summary>
    /// Parquet 文件写入选项
    /// </summary>
    public ParquetOptions ParquetOptions { get; set; } = new ParquetOptions();

    /// <summary>
    /// 压缩方法
    /// </summary>
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Gzip;

    /// <summary>
    /// 压缩级别（部分压缩方法支持）
    /// </summary>
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

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
}
