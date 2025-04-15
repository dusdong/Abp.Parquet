# ParquetFileWriter ��˵�������ʵ��

## 1. ���

`ParquetFileWriter<T>` ��һ�������ܡ����ķ����࣬ר�����ڽ� .NET ����д�� Parquet ��ʽ�ļ���Parquet ��һ�������е����ݴ洢��ʽ���ڴ����ݷ��������й㷺ʹ�ã��ṩ��Ч��ѹ���ͱ��뷽���������ʺ������ݲֿ�ͷ���ϵͳ��

### 1.1 ��Ҫ����

- **����֧��**�����Խ��κ� .NET ��д�� Parquet ��ʽ
- **�Զ��ܹ�����**�����ݶ��������Զ����� Parquet �ܹ�
- **��������**��֧�ָ�Ч����������д��
- **��ʽ����**��֧����ʽ����������ݼ���������ڴ�ռ��
- **�첽����**��ȫ��֧���첽 API����� I/O Ч��
- **��ȡ������**��֧��ͨ��ȡ��������ֹ��ʱ�����е�д�����
- **�ļ�׷��**��֧��׷��д������ Parquet �ļ�
- **��ϸ������**���ṩ�ḻ���쳣��Ϣ
- **�����Ż�**��ʹ��Ԥ��������Է��������������������

## 2. ʹ��˵��

### 2.1 �����÷�

��򵥵�ʹ�÷�ʽ��ʹ�þ�̬�� `CreateAsync` ��������ʵ����Ȼ����� `WriteAsync` ����д�����ݣ�

```csharp
// ׼������
var records = new List<MyDataClass>
{
    new MyDataClass { Id = 1, Name = "Item 1", Value = 10.5 },
    new MyDataClass { Id = 2, Name = "Item 2", Value = 20.3 }
};

// д�� Parquet �ļ�
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
await writer.WriteAsync(records);
```

### 2.2 �Զ�������

����ͨ�� `ParquetWriterConfiguration` ������д����Ϊ��

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

### 2.3 ��ʽ����������ݼ�

������������������ڴ�����ʱ������ʹ����ʽ����

```csharp
// �������ȱ�����
var progress = new Progress<(int ProcessedCount, TimeSpan ElapsedTime)>(p => 
    Console.WriteLine($"�Ѵ��� {p.ProcessedCount} ����¼����ʱ {p.ElapsedTime}"));

// ��ʽ��������ݼ�
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("large_data.parquet");
await writer.WriteStreamingAsync(
    GetDataStream(),  // ���� IEnumerable<MyDataClass> �ķ���
    batchSize: 10000, 
    progress: progress, 
    cancellationToken: token);
```

### 2.4 ʹ���Ѵ򿪵���

��ĳЩ�����£���������ϵͳ���ɻ��ƴ洢�����������д򿪵�����

```csharp
using var fileStream = new FileStream("data.parquet", FileMode.Create);
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync(fileStream);
await writer.WriteAsync(records);
// ע�⣺fileStream ���ᱻ writer �ͷţ���Ҫ�Լ�����
```

## 3. �߼�����

### 3.1 �Զ���ܹ�

�����Ҫ����ȷ�ؿ��� Parquet �ܹ��������ṩ�Զ���ܹ���

```csharp
// �����Զ���ܹ�
var fields = new Field[]
{
    new DataField<int>("Id"),
    new DataField<string>("Name"),
    new DataField<double?>("Value", nullable: true)
};
var schema = new ParquetSchema(fields);

// ʹ���Զ���ܹ�����д����
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("custom_schema.parquet", schema);
await writer.WriteAsync(records);
```

### 3.2 ����ȡ������

��ʱ�����е�д���������ͨ��ȡ������ȡ����

```csharp
var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromMinutes(5)); // 5���Ӻ��Զ�ȡ��

try
{
    using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
    await writer.WriteAsync(records, cts.Token);
}
catch (ParquetOperationCanceledException ex)
{
    Console.WriteLine($"д�������ȡ��: {ex.Message}");
}
```

### 3.3 ������

���ṩ��ר�ŵ��쳣���ͣ�����������⣺

```csharp
try
{
    using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
    await writer.WriteAsync(records);
}
catch (ParquetWriterException ex)
{
    Console.WriteLine($"д�����: {ex.Message}");
    Console.WriteLine($"����: {ex.Operation}, �ļ�: {ex.FilePath}");
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
    Console.WriteLine($"����ת������: {ex.Message}");
    Console.WriteLine($"�ֶ�: {ex.FieldName}, ԭ����: {ex.SourceType}, Ŀ������: {ex.TargetType}");
}
```

## 4. ���ʵ��

### 4.1 �����Ż�

1. **��������������ݼ�**
    - Ĭ���������С(10000)�����ڴ���������������Ը��ݼ�¼�ṹ����
    - ���ڴ����ֶλ��Ӷ���ʹ�ý�С���������С
    - ���ڼ򵥶��󣬿��ʵ������������С

2. **ʹ���첽����**
    - ʼ������ʹ���첽���� (`WriteAsync`��`WriteStreamingAsync`)�����������߳�
    - ���첽�����е����첽�����������첽������������

3. **��Դ����**
    - ʹ�� `using` ���ȷ����Դ��ȷ�ͷ�
    - ���ڳ�ʱ�����е�Ӧ�ã�������ʽ���� `DisposeAsync`

4. **�����������**
    - ʵ�����Բ��ԣ��������������ֲ�ʽ������
    - ��¼��ϸ������Ϣ�����ں������

### 4.2 �ڴ����

1. **��ʽ����**
    - ����������ݼ�ʱʹ�� `WriteStreamingAsync` ����һ���Լ���ȫ������
    - ���ú��ʵ��������С��ƽ���ڴ�ʹ�ú�����

2. **��ʱ�ͷŴ��Ͷ���**
    - д����ɺ�ʱ�ͷŲ�����Ҫ�Ĵ��ͼ���
    - ����ʹ�� `GC.Collect()` ����������������ڴ�

### 4.3 �ļ�����

1. **����ʹ��׷��ģʽ**
    - ׷��ģʽ�ʺ���־���������ݣ�����Ӱ��ѹ��Ч��
    - ������Ҫ��ѹ���ʵ����ݣ����Ƕ��ڴ������ļ����ǲ���׷��

2. **�ļ�����Լ��**
    - ʹ���й��ɵ��ļ�����Լ��������������ڻ�汾��Ϣ
    - ʾ����`data_20230501_v1.parquet`

3. **Ԫ���ݹ���**
    - ������� `CustomMetadata` �洢Ԫ������Ϣ
    - ��¼����ʱ�䡢����Դ���汾����Ϣ

### 4.4 ���̻߳���

1. **�̰߳�ȫ��**
    - `ParquetFileWriter<T>` ʵ�������̰߳�ȫ�ģ���Ҫ�ڶ��̼߳乲��ͬһʵ��
    - ÿ���߳�Ӧ�����Լ���д����ʵ����д�벻ͬ���ļ�

2. **���д���**
    - ���Բ��д�������ת����Ȼ��ʹ��ͬ������Э��д��
    - ʾ����ʹ�� `Parallel.ForEach` �������ݣ�Ȼ��˳��д��

## 5. �����ų�

### 5.1 ��������

1. **"�޷������� X ת��Ϊ���� Y"**
    - ԭ������������ܹ����岻ƥ��
    - ����������Զ���ܹ���ȷ���������ͼ���

2. **"�ֶ�δ���������ҵ�"**
    - ԭ�򣺼ܹ���������û�е��ֶ�
    - ������������ܹ����壬ȷ���ֶ���������������Ӧ

3. **�ڴ�ʹ�ù���**
    - ԭ��һ���Լ��ع�������
    - ���������ʹ����ʽ������ʵ����������С

4. **�ļ���������**
    - ԭ���ļ�����������ʹ��
    - ���������ȷ���ر�֮ǰ�ľ����ʹ���ʵ��� `FileShare` ģʽ

### 5.2 ��Ϲ���

1. **��־��¼**
    - ��¼�ؼ������ʹ���
    - ���񲢼�¼�쳣������

2. **���ܼ��**
    - ʹ�� `Stopwatch` ��������ʱ��
    - ����ڴ�ʹ�����

## 6. ����ʾ��

### 6.1 �� DuckDB ����

```csharp
// д�����ݵ� Parquet
using var writer = await ParquetFileWriter<MyDataClass>.CreateAsync("data.parquet");
await writer.WriteAsync(records);

// ʹ�� DuckDB ��ѯ Parquet �ļ�
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

### 6.2 �����ݴ������̼���

```csharp
// ETL ����ʾ��
public async Task ExtractTransformLoadAsync(string sourceConnectionString, string outputFilePath)
{
    // ��ȡ
    var data = await ExtractDataAsync(sourceConnectionString);
    
    // ת��
    var transformed = data.Select(TransformRecord).ToList();
    
    // ����
    using var writer = await ParquetFileWriter<TransformedRecord>.CreateAsync(outputFilePath);
    await writer.WriteAsync(transformed);
}
```

## 7. ����

`ParquetFileWriter<T>` ��һ��ǿ�����Ĺ��ߣ����ڽ� .NET �����Ч��д�� Parquet ��ʽ�ļ���ͨ����ѭ���ĵ��е����ʵ�������Գ�ַ������������ƣ�ͬʱ���ⳣ�������塣�����Ǵ���С�����ݼ����Ǵ��ģ���ݼ������඼�ṩ�����Ƶ� API ֧�֣��ʺϸ������ݴ�������
