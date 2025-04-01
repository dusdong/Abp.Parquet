# Abp.Parquet

Abp.Parquet 是一个适用于 ABP 框架的高性能 Parquet 文件处理库，提供了简单直观的 API 用于 Parquet 文件的读写操作。该库直接基于 [Parquet.Net](https://github.com/aloneguid/parquet-dotnet)，不依赖于其他 ETL 框架。

## 📦 安装

使用 NuGet 包管理器安装：

```bash
# .NET CLI
dotnet add package Abp.Parquet

# Package Manager
Install-Package Abp.Parquet
```

## ✨ 主要特性

- **零依赖**：除 Parquet.Net 外无其他依赖，轻量级且高效
- **类型安全**：支持强类型实体的读写
- **自动架构生成**：根据实体类自动生成 Parquet 架构
- **追加模式**：支持向现有 Parquet 文件追加数据
- **完整 ABP 集成**：与 ABP 框架无缝集成，支持依赖注入
- **异步支持**：所有操作均提供异步 API
- **高性能**：针对大数据量进行了优化

## 🚀 快速开始

### 基本用法

```csharp
// 注册服务（在模块配置中）
Configure<AbpParquetOptions>(options =>
{
    options.DefaultCompressionMethod = CompressionMethod.Snappy;
});

// 使用示例
public class MyAppService : ApplicationService
{
    private readonly IParquetArchiveHandler _parquetHandler;
    
    public MyAppService(IParquetArchiveHandler parquetHandler)
    {
        _parquetHandler = parquetHandler;
    }
    
    public async Task ExportDataAsync()
    {
        var records = new List<UserDto>
        {
            new UserDto { Id = 1, Name = "张三", CreateTime = DateTime.Now },
            new UserDto { Id = 2, Name = "李四", CreateTime = DateTime.Now }
        };
        
        // 写入 Parquet 文件
        await _parquetHandler.WriteToFileAsync("users.parquet", records);
        
        // 读取 Parquet 文件
        var loadedUsers = await _parquetHandler.ReadFromFileAsync<UserDto>("users.parquet");
    }
}
```

### 高级配置

```csharp
// 创建自定义配置
var options = new ParquetWriterOptions
{
    CompressionMethod = CompressionMethod.Gzip,
    CompressionLevel = 6,
    TreatDateTimeAsDateTimeOffset = true,
    IgnoredFields = new HashSet<string> { "IgnoredProperty" },
    CustomMetadata = new Dictionary<string, string>
    {
        ["Creator"] = "Abp.Parquet",
        ["CreatedAt"] = DateTime.Now.ToString("o")
    }
};

// 使用自定义配置写入
await _parquetHandler.WriteToFileAsync("data.parquet", records, options);
```

## 📚 主要组件

### ParquetFileWriter<T>

用于将强类型数据写入 Parquet 文件：

```csharp
// 创建写入器
using var writer = new ParquetFileWriter<UserDto>("users.parquet");

// 写入数据
await writer.WriteAsync(users);
```

### ParquetSchemaGenerator<T>

根据实体类型自动生成 Parquet 架构：

```csharp
// 创建架构生成器
var generator = new ParquetSchemaGenerator<UserDto>();

// 生成架构
var schema = generator.GenerateSchema();
```

### ParquetArchiveFormatHandler

提供高级文件处理功能：

```csharp
var handler = new ParquetArchiveFormatHandler();

// 写入文件
await handler.WriteToFileAsync("data.parquet", records);

// 读取文件
var data = await handler.ReadFromFileAsync<MyEntity>("data.parquet");

// 获取架构
var schema = await handler.GetSchemaAsync("data.parquet");
```

## ⚙️ 配置选项

| 属性 | 描述 | 默认值 |
|------|------|---------|
| CompressionMethod | Parquet 压缩方法 | Snappy |
| CompressionLevel | 压缩级别 | 0 |
| TreatDateTimeAsDateTimeOffset | 是否将 DateTime 视为 DateTimeOffset | false |
| TreatDateTimeAsString | 是否将 DateTime 视为字符串 | false |
| TreatDateTimeOffsetAsString | 是否将 DateTimeOffset 视为字符串 | false |
| IgnoredFields | 要忽略的字段集合 | 空集合 |
| CustomMetadata | 自定义元数据 | 空字典 |

## 🔍 与 ChoETL 的对比

| 功能 | Abp.Parquet | ChoETL |
|------|-------------|--------|
| 依赖 | 仅 Parquet.Net | 多个依赖 |
| 集成 | 专为 ABP 框架设计 | 通用 ETL 框架 |
| 体积 | 轻量级 | 较重 |
| 性能 | 经过优化，直接使用 Parquet.Net | 通过中间层调用 |
| 维护 | 专注于 Parquet 格式 | 支持多种格式 |

## 🤝 贡献

欢迎贡献代码、报告问题或提供改进建议。请遵循以下步骤：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开 Pull Request

## 📄 许可证

该项目采用 MIT 许可证 - 详情请参阅 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) - 提供强大的 Parquet 文件处理能力
- [ABP Framework](https://abp.io/) - 提供现代化的 .NET 应用框架

---

🔗 [查看文档](https://github.com/yourusername/Abp.Parquet/wiki) | ⭐ [GitHub 仓库](https://github.com/yourusername/Abp.Parquet) | 📦 [NuGet 包](https://www.nuget.org/packages/Abp.Parquet)