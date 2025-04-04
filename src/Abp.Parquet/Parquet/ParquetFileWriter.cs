using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Abp.Parquet;

/// <summary>
/// 负责将数据写入 Parquet 文件，支持自动生成架构和追加写入。
/// </summary>
/// <typeparam name="T">记录类型。</typeparam>
public class ParquetFileWriter<T> : IDisposable, IAsyncDisposable
{
    // 默认分块大小
    private const int DefaultBatchSize = 10000;

    private readonly string _filePath;
    private readonly ParquetSchema _schema;
    private readonly ParquetWriterConfiguration _configuration;
    private readonly PropertyAccessor<T> _propertyAccessor;
    private readonly PropertyInfo[] _properties;
    private readonly ParquetSchemaGenerator<T> _schemaGenerator;

    // 字段名称与原始属性类型的映射
    private readonly IDictionary<string, Type> _fieldOriginalTypes;

    // 持久化的 FileStream 和 ParquetWriter
    private FileStream _fileStream;
    private ParquetWriter _parquetWriter;
    private bool _isDisposed = false;

    // 用于懒初始化的信号量
    private readonly SemaphoreSlim _initSemaphore = new SemaphoreSlim(1, 1);
    private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);
    private bool _isInitialized = false;

    // 缓冲区池
    private static readonly BufferPool _bufferPool = new BufferPool(1024 * 1024, 20); // 1MB缓冲区

    /// <summary>
    /// 私有构造函数，由CreateAsync工厂方法调用
    /// </summary>
    private ParquetFileWriter(string filePath, ParquetSchema schema = null, ParquetWriterConfiguration configuration = null)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("文件路径不能为空。", nameof(filePath));

        _filePath = filePath;
        _configuration = configuration ?? new ParquetWriterConfiguration();

        if (schema != null)
        {
            _schema = schema;
            _fieldOriginalTypes = ExtractFieldTypesFromSchema(_schema);
        }
        else
        {
            _schemaGenerator = new ParquetSchemaGenerator<T>(_configuration);
            _schema = _schemaGenerator.GenerateSchema();
            _fieldOriginalTypes = new Dictionary<string, Type>();

            // 获取字段的原始属性类型
            foreach (var field in _schema.Fields.OfType<DataField>())
            {
                string fieldName = field.Name;
                Type originalType = _schemaGenerator.GetOriginalFieldType(fieldName);
                _fieldOriginalTypes[fieldName] = originalType;
            }
        }

        // 使用反射获取类型 T 的所有公共属性
        _properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
        // 初始化高性能属性访问器
        _propertyAccessor = new PropertyAccessor<T>(_properties);
    }

    /// <summary>
    /// 创建 ParquetFileWriter 的异步工厂方法
    /// </summary>
    /// <param name="filePath">目标 Parquet 文件路径</param>
    /// <param name="schema">可选的 Parquet 架构，如果为 null，则自动生成</param>
    /// <param name="configuration">可选的配置对象，如果为 null，则使用默认配置</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>初始化完成的 ParquetFileWriter 实例</returns>
    public static async Task<ParquetFileWriter<T>> CreateAsync(
        string filePath,
        ParquetSchema schema = null,
        ParquetWriterConfiguration configuration = null,
        CancellationToken cancellationToken = default)
    {
        var writer = new ParquetFileWriter<T>(filePath, schema, configuration);
        await writer.InitializeAsync(cancellationToken).ConfigureAwait(false);
        return writer;
    }

    /// <summary>
    /// 异步初始化 FileStream 和 ParquetWriter。
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    private async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_isInitialized)
            return;

        await _initSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_isInitialized)
                return;

            cancellationToken.ThrowIfCancellationRequested();

            var fileExists = File.Exists(_filePath);
            var context = new Dictionary<string, object>
            {
                { "FileExists", fileExists }
            };

            try
            {
                // 确保目录存在
                var directory = Path.GetDirectoryName(_filePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // 打开 FileStream - 这是真正的异步IO操作
                _fileStream = new FileStream(
                    _filePath,
                    fileExists ? FileMode.Open : FileMode.Create,
                    FileAccess.ReadWrite,
                    FileShare.Read,
                    4096, // 默认缓冲区大小
                    FileOptions.Asynchronous); // 启用异步IO

                if (fileExists)
                {
                    // 如果是追加模式，确保流指针位于文件末尾
                    _fileStream.Seek(0, SeekOrigin.End);
                }

                cancellationToken.ThrowIfCancellationRequested();

                // 创建ParquetWriter - 使用异步方法
                _parquetWriter = await ParquetWriter.CreateAsync(_schema, _fileStream, append: fileExists);

                _parquetWriter.CompressionMethod = _configuration.CompressionMethod;
                _parquetWriter.CompressionLevel = _configuration.CompressionLevel;

                if (_configuration.CustomMetadata != null)
                    _parquetWriter.CustomMetadata = _configuration.CustomMetadata;

                _isInitialized = true;
            }
            catch (OperationCanceledException)
            {
                // 清理资源并传播取消异常
                await CleanupResourcesAsync();
                throw new ParquetOperationCanceledException("Parquet 文件初始化操作已取消。", 
                    "Initialize", _filePath);
            }
            catch (Exception ex)
            {
                // 确保在初始化失败时释放资源
                await CleanupResourcesAsync();

                if (ex is UnauthorizedAccessException uae)
                {
                    throw new ParquetWriterException($"没有文件写入权限: {uae.Message}",
                        "Initialize", _filePath, context, uae);
                }
                    
                if (ex is IOException ioEx)
                {
                    throw new ParquetWriterException($"初始化 Parquet 写入器时发生 I/O 错误: {ioEx.Message}", 
                        "Initialize", _filePath, context, ioEx);
                }

                throw new ParquetWriterException($"初始化 Parquet 写入器失败: {ex.Message}", 
                    "Initialize", _filePath, context, ex);
            }
        }
        finally
        {
            _initSemaphore.Release();
        }
    }

    /// <summary>
    /// 清理资源
    /// </summary>
    private async Task CleanupResourcesAsync()
    {
        await ResourceManager.SafeDisposeAsync(_parquetWriter);
        await ResourceManager.SafeCloseStreamAsync(_fileStream);
            
        _parquetWriter = null;
        _fileStream = null;
    }

    /// <summary>
    /// 从 ParquetSchema 中提取字段原始类型
    /// </summary>
    private static IDictionary<string, Type> ExtractFieldTypesFromSchema(ParquetSchema schema)
    {
        var fieldTypes = new Dictionary<string, Type>();
        foreach (var field in schema.Fields.OfType<DataField>())
        {
            // 如果是 Nullable<>, 获取基础类型；否则直接使用 ClrType
            Type baseType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
            fieldTypes[field.Name] = baseType;
        }

        return fieldTypes;
    }

    /// <summary>
    /// 写入数据到 Parquet 文件 (同步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    public void Write(List<T> records)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保避免死锁 - 使用Task.Run包装整个异步操作
        Task.Run(async () => 
        {
            await InitializeAsync().ConfigureAwait(false);
            await InternalWriteAsync(records, CancellationToken.None).ConfigureAwait(false);
        }).GetAwaiter().GetResult();
    }

    /// <summary>
    /// 写入数据到 Parquet 文件 (异步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>完成任务</returns>
    public async Task WriteAsync(List<T> records, CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保初始化
        await InitializeAsync(cancellationToken).ConfigureAwait(false);
            
        // 调用共享的内部写入方法
        await InternalWriteAsync(records, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 内部共享写入逻辑
    /// </summary>
    private async Task InternalWriteAsync(List<T> records, CancellationToken cancellationToken)
    {
        await _writeSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // 如果数据量很大，分批处理
            if (records.Count > _configuration.BatchSize)
            {
                await WriteBatchedAsync(records, cancellationToken).ConfigureAwait(false);
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();

            // 创建行组 - 同步调用
            var rowGroupWriter = _parquetWriter.CreateRowGroup();
            using (rowGroupWriter)
            {
                // 构建字段名称到 DataField 的字典
                var dataFieldDict = _schema.Fields
                    .OfType<DataField>()
                    .ToDictionary(f => f.Name, f => f);

                // 自动创建 DataColumn 列表 - 这是CPU密集型的，放在后台线程执行
                var dataColumns = await Task.Run(() => 
                    CreateDataColumns(records, dataFieldDict), cancellationToken);

                // 写入所有列
                foreach (var dataColumn in dataColumns)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await rowGroupWriter.WriteColumnAsync(dataColumn);
                }
            }

            // 异步刷新流
            await _fileStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException("Parquet 写入操作已被取消。", 
                "Write", _filePath);
        }
        catch (Exception ex)
        {
            var context = new Dictionary<string, object>
            {
                { "RecordCount", records.Count },
                { "RecordType", typeof(T).Name }
            };
                
            throw new ParquetWriterException($"写入Parquet数据失败: {ex.Message}", 
                "Write", _filePath, context, ex);
        }
        finally
        {
            _writeSemaphore.Release();
        }
    }

    /// <summary>
    /// 分批异步写入大量记录
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>完成任务</returns>
    private async Task WriteBatchedAsync(List<T> records, CancellationToken cancellationToken)
    {
        int batchSize = _configuration.BatchSize;
        int totalRecords = records.Count;

        // 计算需要多少批次
        int batchCount = (totalRecords + batchSize - 1) / batchSize;
            
        var progressContext = new Dictionary<string, object>
        {
            { "TotalRecords", totalRecords },
            { "BatchSize", batchSize },
            { "BatchCount", batchCount }
        };

        try
        {
            for (int i = 0; i < batchCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int skip = i * batchSize;
                int take = Math.Min(batchSize, totalRecords - skip);
                    
                progressContext["CurrentBatch"] = i + 1;
                progressContext["ProcessedRecords"] = skip + take;
                progressContext["ProgressPercent"] = ((skip + take) * 100.0) / totalRecords;

                // 获取当前批次的数据
                var batch = records.GetRange(skip, take);

                // 写入批次数据
                var rowGroupWriter = _parquetWriter.CreateRowGroup();
                using (rowGroupWriter)
                {
                    // 构建字段名称到 DataField 的字典
                    var dataFieldDict = _schema.Fields
                        .OfType<DataField>()
                        .ToDictionary(f => f.Name, f => f);

                    // 自动创建 DataColumn 列表 - CPU密集型，放在后台线程执行
                    var dataColumns = await Task.Run(() => 
                        CreateDataColumns(batch, dataFieldDict), cancellationToken);

                    // 写入所有列
                    foreach (var dataColumn in dataColumns)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await rowGroupWriter.WriteColumnAsync(dataColumn);
                    }
                }
            }

            // 最后异步刷新流，确保所有数据写入
            await _fileStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new ParquetWriterException($"批处理写入失败: {ex.Message}", 
                "WriteBatched", _filePath, progressContext, ex);
        }
    }

    /// <summary>
    /// 支持流式处理的大数据集异步写入
    /// </summary>
    /// <param name="records">要写入的数据流</param>
    /// <param name="batchSize">批处理大小</param>
    /// <param name="progress">进度报告</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>完成任务</returns>
    public async Task WriteStreamingAsync(
        IEnumerable<T> records,
        int batchSize = DefaultBatchSize,
        IProgress<(int ProcessedCount, TimeSpan ElapsedTime)> progress = null,
        CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null)
            return;

        // 确保初始化
        await InitializeAsync(cancellationToken).ConfigureAwait(false);

        await _writeSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // 使用可重用的缓冲区收集记录
            var buffer = new List<T>(batchSize);
            int processedCount = 0;
            var stopwatch = Stopwatch.StartNew();

            foreach (var record in records)
            {
                cancellationToken.ThrowIfCancellationRequested();

                buffer.Add(record);
                processedCount++;

                // 当达到批处理大小时写入
                if (buffer.Count >= batchSize)
                {
                    await WriteBufferAsync(buffer, cancellationToken);
                        
                    // 报告进度
                    progress?.Report((processedCount, stopwatch.Elapsed));
                        
                    buffer.Clear();
                }
            }

            // 写入剩余记录
            if (buffer.Count > 0)
            {
                await WriteBufferAsync(buffer, cancellationToken);
                    
                // 最终进度报告
                progress?.Report((processedCount, stopwatch.Elapsed));
            }
        }
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException("Parquet 流式写入操作已被取消。", 
                "WriteStreaming", _filePath);
        }
        catch (Exception ex)
        {
            throw new ParquetWriterException($"流式写入 Parquet 数据失败: {ex.Message}", 
                "WriteStreaming", _filePath, null, ex);
        }
        finally
        {
            _writeSemaphore.Release();
        }
    }

    /// <summary>
    /// 将缓冲区中的记录写入文件
    /// </summary>
    private async Task WriteBufferAsync(List<T> buffer, CancellationToken cancellationToken)
    {
        var rowGroupWriter = _parquetWriter.CreateRowGroup();
        using (rowGroupWriter)
        {
            // 构建字段名称到 DataField 的字典
            var dataFieldDict = _schema.Fields
                .OfType<DataField>()
                .ToDictionary(f => f.Name, f => f);

            // 自动创建 DataColumn 列表
            var dataColumns = await Task.Run(() => CreateDataColumns(buffer, dataFieldDict), cancellationToken);

            // 写入所有列
            foreach (var dataColumn in dataColumns)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await rowGroupWriter.WriteColumnAsync(dataColumn);
            }
        }
    }

    /// <summary>
    /// 自动创建 DataColumn 列表。
    /// </summary>
    /// <param name="records">要写入的数据列表。</param>
    /// <param name="dataFieldDict">字段名称到 DataField 的字典。</param>
    /// <returns>DataColumn 列表。</returns>
    private List<DataColumn> CreateDataColumns(List<T> records, Dictionary<string, DataField> dataFieldDict)
    {
        var dataColumns = new List<DataColumn>();
        foreach (var field in _schema.Fields.OfType<DataField>())
        {
            string fieldName = field.Name;
            // 获取原始属性类型
            if (!_fieldOriginalTypes.TryGetValue(fieldName, out Type originalPropertyType))
            {
                Debug.WriteLine($"警告: 原始类型未找到 for field '{fieldName}'。");
                continue;
            }

            // 检查是否有对应的属性访问器
            if (!_propertyAccessor.HasProperty(fieldName))
            {
                Debug.WriteLine($"警告: 属性 '{fieldName}' 在类型 '{typeof(T).Name}' 中未找到，字段将被忽略。");
                continue;
            }

            try
            {
                // 使用预编译的属性访问器提取字段值 - 比反射快很多
                var rawValues = records.Select(r => _propertyAccessor.GetValue(r, fieldName)).ToArray();
                Array fieldValues;

                if (field.IsNullable)
                {
                    fieldValues = ConvertToNullableArray(rawValues, fieldName, originalPropertyType, field.ClrType);
                }
                else
                {
                    fieldValues = ConvertToNonNullableArray(rawValues, fieldName, field.ClrType);
                }

                // 创建 DataColumn 并添加到列表
                var column = new DataColumn(field, fieldValues);
                dataColumns.Add(column);
            }
            catch (ParquetWriterException)
            {
                // 直接重新抛出 Parquet 异常
                throw;
            }
            catch (Exception ex)
            {
                throw new ParquetWriterException($"写入字段 '{fieldName}' 时发生错误: {ex.Message}", 
                    "CreateDataColumns", null, 
                    new Dictionary<string, object> { { "FieldName", fieldName } }, ex);
            }
        }

        return dataColumns;
    }

    /// <summary>
    /// 转换为可空数组
    /// </summary>
    private Array ConvertToNullableArray(object[] rawValues, string fieldName, Type originalPropertyType, Type targetType)
    {
        // 处理可空值类型（如 int?）
        if (originalPropertyType.IsValueType && Nullable.GetUnderlyingType(originalPropertyType) != null)
        {
            Type baseType = Nullable.GetUnderlyingType(originalPropertyType);
            if (baseType == null)
            {
                throw new ParquetWriterException($"无法获取字段 '{fieldName}' 的基础类型。");
            }

            // 创建 Nullable<T> 数组
            var typedArray = Array.CreateInstance(originalPropertyType, rawValues.Length);
            for (int i = 0; i < rawValues.Length; i++)
            {
                if (rawValues[i] != null)
                {
                    try
                    {
                        typedArray.SetValue(Convert.ChangeType(rawValues[i], baseType), i);
                    }
                    catch (Exception ex)
                    {
                        throw new ParquetTypeConversionException(
                            $"转换类型失败: {rawValues[i]} -> {baseType.Name}",
                            rawValues[i]?.GetType(), baseType, fieldName, "TypeConversion", null, ex);
                    }
                }
                else
                {
                    typedArray.SetValue(null, i);
                }
            }

            return typedArray;
        }
        else if (!originalPropertyType.IsValueType)
        {
            // 处理引用类型（如 string）
            if (originalPropertyType == typeof(string))
            {
                return rawValues.Select(val => val?.ToString()).ToArray();
            }
            else
            {
                // 为其他引用类型创建适当的数组
                var resultArray = Array.CreateInstance(targetType, rawValues.Length);
                for (int i = 0; i < rawValues.Length; i++)
                {
                    if (rawValues[i] != null)
                    {
                        try
                        {
                            resultArray.SetValue(Convert.ChangeType(rawValues[i], targetType), i);
                        }
                        catch (Exception ex)
                        {
                            throw new ParquetTypeConversionException(
                                $"转换引用类型失败: {rawValues[i]} -> {targetType.Name}",
                                rawValues[i]?.GetType(), targetType, fieldName, "TypeConversion", null, ex);
                        }
                    }
                    else
                    {
                        resultArray.SetValue(null, i);
                    }
                }
                return resultArray;
            }
        }
        else
        {
            throw new ParquetWriterException($"字段 '{fieldName}' 的类型无法处理: {originalPropertyType.Name}");
        }
    }

    /// <summary>
    /// 转换为非可空数组
    /// </summary>
    private Array ConvertToNonNullableArray(object[] rawValues, string fieldName, Type targetType)
    {
        // 创建具体类型的数组
        var typedArray = Array.CreateInstance(targetType, rawValues.Length);
            
        for (int i = 0; i < rawValues.Length; i++)
        {
            if (rawValues[i] != null)
            {
                try
                {
                    typedArray.SetValue(Convert.ChangeType(rawValues[i], targetType), i);
                }
                catch (Exception ex)
                {
                    throw new ParquetTypeConversionException(
                        $"转换类型失败: {rawValues[i]} -> {targetType.Name}",
                        rawValues[i]?.GetType(), targetType, fieldName, "TypeConversion", null, ex);
                }
            }
            else
            {
                // 使用默认值避免 null
                typedArray.SetValue(GetDefault(targetType), i);
            }
        }
            
        return typedArray;
    }

    /// <summary>
    /// 获取指定类型的默认值。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static object GetDefault(Type type)
    {
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>
    /// 实现 IAsyncDisposable 接口，异步释放资源
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
            return;
                
        _isInitialized = false;
        await CleanupResourcesAsync();
            
        ResourceManager.SafeDispose(_initSemaphore);
        ResourceManager.SafeDispose(_writeSemaphore);
            
        _isDisposed = true;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 实现 IDisposable 接口，同步释放资源
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed)
            return;
                
        _isInitialized = false;
            
        ResourceManager.SafeDispose(_parquetWriter);
        ResourceManager.SafeDispose(_fileStream);
        ResourceManager.SafeDispose(_initSemaphore);
        ResourceManager.SafeDispose(_writeSemaphore);
            
        _isDisposed = true;
        GC.SuppressFinalize(this);
    }
}
