# ParquetFileWriter 类说明及最佳实践

## 1. 简介

`ParquetFileWriter<T>` 是一个高性能、灵活的泛型类，专门用于将 .NET 对象写入 Parquet 格式文件。Parquet 是一种面向列的数据存储格式，在大数据分析场景中广泛使用，提供高效的压缩和编码方案，尤其适合于数据仓库和分析系统。

### 1.1 主要特性

- **泛型支持**：可以将任何 .NET 类写入 Parquet 格式
- **自动架构生成**：根据对象属性自动创建 Parquet 架构
- **批量处理**：支持高效的批量数据写入
- **流式处理**：支持流式处理大型数据集，避免大内存占用
- **异步操作**：全面支持异步 API，提高 I/O 效率
- **可取消操作**：支持通过取消令牌终止长时间运行的写入操作
- **文件追加**：支持追加写入现有 Parquet 文件
- **详细错误处理**：提供丰富的异常信息
- **性能优化**：使用预编译的属性访问器和任务并行提高性能

## 2. 使用说明

### 2.1 基本用法

最简单的使用方式是使用静态的 `CreateAsync` 方法创建实例，然后调用 `WriteAsync` 方法写入数据：

```csharp
// 准备数据
var records = new List<MyDataClass>
{
    new MyDataClass { Id = 1, Name = "Item 1", Value = 10.5 },
    new MyDataClass { Id = 2, Name = "Item 2", Value = 20.3 }
};

// 写入 Parquet 文件
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
await writer.WriteAsync(records);
```

### 2.2 自定义配置

可以通过 `ParquetWriterConfiguration` 类配置写入行为：

```csharp
var config = new ParquetWriterConfiguration
{
    CompressionMethod = CompressionMethod.Gzip,
    CompressionLevel = CompressionLevel.Optimal,
    BatchSize = 5000,
    CustomMetadata = new Dictionary<string, string>
    {
        { "CreatedBy", "MyApplication" },
        { "CreatedOn", DateTime.UtcNow.ToString("o") }
    }
};

using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet", null, config);
await writer.WriteAsync(records);
```

### 2.3 流式处理大型数据集

当处理的数据量超过内存容量时，可以使用流式处理：

```csharp
// 创建进度报告器
var progress = new Progress<(int ProcessedCount, TimeSpan ElapsedTime)>(p => 
    Console.WriteLine($"已处理 {p.ProcessedCount} 条记录，耗时 {p.ElapsedTime}"));

// 流式处理大数据集
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("large_data.parquet");
await writer.WriteStreamingAsync(
    GetDataStream(),  // 返回 IEnumerable<MyDataClass> 的方法
    batchSize: 10000, 
    progress: progress, 
    cancellationToken: token);
```

### 2.4 使用已打开的流

在某些场景下（如与其他系统集成或云存储），可能已有打开的流：

```csharp
using var fileStream = new FileStream("data.parquet", FileMode.Create);
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync(fileStream);
await writer.WriteAsync(records);
// 注意：fileStream 不会被 writer 释放，需要自己管理
```

## 3. 高级主题

### 3.1 自定义架构

如果需要更精确地控制 Parquet 架构，可以提供自定义架构：

```csharp
// 创建自定义架构
var fields = new Field[]
{
    new DataField<int>("Id"),
    new DataField<string>("Name"),
    new DataField<double?>("Value", nullable: true)
};
var schema = new ParquetSchema(fields);

// 使用自定义架构创建写入器
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("custom_schema.parquet", schema);
await writer.WriteAsync(records);
```

### 3.2 处理取消操作

长时间运行的写入操作可以通过取消令牌取消：

```csharp
var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromMinutes(5)); // 5分钟后自动取消

try
{
    using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
    await writer.WriteAsync(records, cts.Token);
}
catch (ParquetOperationCanceledException ex)
{
    Console.WriteLine($"写入操作被取消: {ex.Message}");
}
```

### 3.3 错误处理

库提供了专门的异常类型，帮助诊断问题：

```csharp
try
{
    using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
    await writer.WriteAsync(records);
}
catch (ParquetWriterException ex)
{
    Console.WriteLine($"写入错误: {ex.Message}");
    Console.WriteLine($"操作: {ex.Operation}, 文件: {ex.FilePath}");
    if (ex.Context != null)
    {
        foreach (var kvp in ex.Context)
        {
            Console.WriteLine($"{kvp.Key}: {kvp.Value}");
        }
    }
}
catch (ParquetTypeConversionException ex)
{
    Console.WriteLine($"类型转换错误: {ex.Message}");
    Console.WriteLine($"字段: {ex.FieldName}, 原类型: {ex.SourceType}, 目标类型: {ex.TargetType}");
}
```

## 4. 最佳实践

### 4.1 性能优化

1. **分批处理大型数据集**
    - 默认批处理大小(10000)适用于大多数场景，但可以根据记录结构调整
    - 对于大型字段或复杂对象，使用较小的批处理大小
    - 对于简单对象，可适当增大批处理大小

2. **使用异步方法**
    - 始终优先使用异步方法 (`WriteAsync`、`WriteStreamingAsync`)，避免阻塞线程
    - 在异步方法中调用异步方法，保持异步环境的完整性

3. **资源管理**
    - 使用 `using` 语句确保资源正确释放
    - 对于长时间运行的应用，考虑显式调用 `DisposeAsync`

4. **错误处理和重试**
    - 实现重试策略，尤其是在网络或分布式环境中
    - 记录详细错误信息，便于后期诊断

### 4.2 内存管理

1. **流式处理**
    - 处理大型数据集时使用 `WriteStreamingAsync` 而非一次性加载全部数据
    - 设置合适的批处理大小，平衡内存使用和性能

2. **及时释放大型对象**
    - 写入完成后及时释放不再需要的大型集合
    - 考虑使用 `GC.Collect()` 在批量操作后回收内存

### 4.3 文件管理

1. **合理使用追加模式**
    - 追加模式适合日志或增量数据，但会影响压缩效率
    - 对于需要高压缩率的数据，考虑定期创建新文件而非不断追加

2. **文件命名约定**
    - 使用有规律的文件命名约定，例如包含日期或版本信息
    - 示例：`data_20230501_v1.parquet`

3. **元数据管理**
    - 充分利用 `CustomMetadata` 存储元数据信息
    - 记录创建时间、数据源、版本等信息

### 4.4 多线程环境

1. **线程安全性**
    - `ParquetFileWriter<T>` 实例不是线程安全的，不要在多线程间共享同一实例
    - 每个线程应创建自己的写入器实例，写入不同的文件

2. **并行处理**
    - 可以并行处理数据转换，然后使用同步机制协调写入
    - 示例：使用 `Parallel.ForEach` 处理数据，然后按顺序写入

## 5. 故障排除

### 5.1 常见问题

1. **"无法从类型 X 转换为类型 Y"**
    - 原因：属性类型与架构定义不匹配
    - 解决方案：自定义架构或确保属性类型兼容

2. **"字段未在类型中找到"**
    - 原因：架构包含类型没有的字段
    - 解决方案：检查架构定义，确保字段名与类属性名对应

3. **内存使用过高**
    - 原因：一次性加载过多数据
    - 解决方案：使用流式处理和适当的批处理大小

4. **文件锁定错误**
    - 原因：文件被其他进程使用
    - 解决方案：确保关闭之前的句柄，使用适当的 `FileShare` 模式

### 5.2 诊断工具

1. **日志记录**
    - 记录关键操作和错误
    - 捕获并记录异常上下文

2. **性能监控**
    - 使用 `Stopwatch` 测量操作时间
    - 监控内存使用情况

## 6. 集成示例

### 6.1 与 DuckDB 集成

```csharp
// 写入数据到 Parquet
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
await writer.WriteAsync(records);

// 使用 DuckDB 查询 Parquet 文件
using var connection = new DuckDBConnection("Data Source=:memory:");
connection.Open();

using var command = connection.CreateCommand();
command.CommandText = "SELECT * FROM 'data.parquet'";
using var reader = command.ExecuteReader();

while (reader.Read())
{
    Console.WriteLine($"Id: {reader.GetInt32(0)}, Name: {reader.GetString(1)}");
}
```

### 6.2 与数据处理流程集成

```csharp
// ETL 流程示例
public async Task ExtractTransformLoadAsync(string sourceConnectionString, string outputFilePath)
{
    // 提取
    var data = await ExtractDataAsync(sourceConnectionString);
    
    // 转换
    var transformed = data.Select(TransformRecord).ToList();
    
    // 加载
    using var writer = await ParquetFileWriter<TransformedRecord>.CreateAsync(outputFilePath);
    await writer.WriteAsync(transformed);
}
```

## 7. 结论

`ParquetFileWriter<T>` 是一个强大、灵活的工具，用于将 .NET 对象高效地写入 Parquet 格式文件。通过遵循本文档中的最佳实践，可以充分发挥其性能优势，同时避免常见的陷阱。无论是处理小型数据集还是大规模数据集，该类都提供了完善的 API 支持，适合各种数据处理场景。
