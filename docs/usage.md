# Abp.Parquet 使用文档

## 1. 基本概念

Parquet 是一种列式存储格式，适合大数据场景。Abp.Parquet 提供了：

- 高性能读写
- 压缩支持
- 元数据管理
- 类型安全

## 2. 核心组件

### 2.1 ParquetFileWriter

```csharp
public class ParquetFileWriter<T> : IDisposable where T : class, new()
{
    public static Task<ParquetFileWriter<T>> CreateAsync(
        string filePath, 
        ParquetWriterConfiguration configuration = null,
        CancellationToken cancellationToken = default);
        
    public Task WriteAsync(List<T> records, CancellationToken cancellationToken = default);
}
```

### 2.2 ParquetFileReader

```csharp
public class ParquetFileReader<T> : IDisposable where T : class, new()
{
    public static Task<ParquetFileReader<T>> CreateAsync(
        string filePath,
        CancellationToken cancellationToken = default);
        
    public IAsyncEnumerable<T> ReadRecordsStreaming(CancellationToken cancellationToken = default);
}
```

## 3. 最佳实践

### 3.1 写入优化

```csharp
// 创建写入配置
var writerConfig = records.Count > 100000
    ? ParquetWriterConfiguration.CreateForLargeDatasets()
    : ParquetWriterConfiguration.CreateForMaximumCompression();

// 添加元数据
writerConfig.CustomMetadata["CreatedBy"] = "Abp.Parquet";
writerConfig.CustomMetadata["CreatedOn"] = DateTime.UtcNow.ToString("o");
```

### 3.2 读取优化

1. 使用流式读取处理大文件
2. 实现分页读取
3. 使用异步方法
4. 合理设置缓冲区大小

### 3.3 性能优化

1. 选择合适的压缩算法
2. 批量写入数据
3. 使用列式存储的优势
4. 实现缓存策略

## 4. 集成使用示例

### 4.1 基础写入示例

```csharp
public class ParquetWriterService
{
    private readonly ILogger _logger;

    public async Task WriteDataAsync<T>(string filePath, List<T> records) where T : class, new()
    {
        var sw = Stopwatch.StartNew();
        var operationId = Guid.NewGuid().ToString("N");
        _logger.Info($"[操作ID: {operationId}] 开始写入Parquet文件，总记录数: {records.Count}");

        var writerConfig = ParquetWriterConfiguration.CreateForLargeDatasets();
        writerConfig.CustomMetadata["CreatedBy"] = "Abp.Parquet";
        writerConfig.CustomMetadata["CreatedOn"] = DateTime.UtcNow.ToString("o");

        try
        {
            using var writer = await ParquetFileWriter<T>.CreateAsync(filePath, writerConfig);
            await writer.WriteAsync(records);
            
            sw.Stop();
            _logger.Info($"[操作ID: {operationId}] 数据写入完成，耗时: {sw.ElapsedMilliseconds}ms");
        }
        catch (Exception ex)
        {
            _logger.Error($"[操作ID: {operationId}] 写入失败: {ex.Message}", ex);
            throw;
        }
    }
}
```

### 4.2 流式读取示例

```csharp
public class ParquetReaderService
{
    private readonly ILogger _logger;

    public async Task ProcessLargeFileAsync<T>(string filePath) where T : class, new()
    {
        try
        {
            using var reader = await ParquetFileReader<T>.CreateAsync(filePath);
            await foreach (var record in reader.ReadRecordsStreaming())
            {
                // 处理每条记录
                ProcessRecord(record);
            }
        }
        catch (Exception ex)
        {
            _logger.Error($"处理文件失败: {ex.Message}", ex);
            throw;
        }
    }

    private void ProcessRecord<T>(T record)
    {
        // 实现记录处理逻辑
    }
}
```

### 4.3 数据归档场景

```csharp
public class ParquetArchiveFormatHandler : IArchiveFormatHandler
{
    public async Task WriteRecordsAsync<T>(string filePath, List<T> records)
    {
        var writerConfig = ParquetWriterConfiguration.CreateForLargeDatasets();
        
        using var writer = await ParquetFileWriter<T>.CreateAsync(filePath, writerConfig);
        await writer.WriteAsync(records);
    }
}
```

## 5. 注意事项

1. 文件路径管理
   - 使用绝对路径
   - 确保目录存在
   - 处理文件权限

2. 错误处理
   - 实现重试机制
   - 记录详细日志
   - 优雅处理异常

3. 资源管理
   - 及时释放资源
   - 使用 using 语句
   - 实现 IDisposable

4. 并发控制
   - 使用文件锁
   - 实现读写分离
   - 控制并发数量

## 6. 常见问题

### 6.1 性能优化

Q: 如何提高 Parquet 文件的读写性能？
A: 建议采取以下措施：
1. 选择合适的压缩算法
2. 批量写入数据
3. 使用流式读取
4. 实现缓存策略
5. 优化数据结构

### 6.2 内存管理

Q: 如何处理大文件的内存问题？
A: 建议采用以下策略：
1. 使用流式读取
2. 实现分页处理
3. 控制批处理大小
4. 及时释放资源

### 6.3 错误处理

Q: 如何处理文件操作错误？
A: 建议实现以下机制：
1. 文件存在性检查
2. 权限验证
3. 重试策略
4. 错误日志记录
5. 监控告警 