using Confluent.Kafka;
using System.IO.Compression;
using System.Text.Json;

namespace MessageBus.Extensions
{
    internal class Serializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            //apenas esta linha bastaria para enviar a mensagem em json no formato de bytes para o kafka
            var bytes = JsonSerializer.SerializeToUtf8Bytes(data);

            //Compressao e compactacao de dados da mensagem
            using var memoryStream = new MemoryStream();
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Compress, true);
            zipStream.Write(bytes, 0, bytes.Length);
            zipStream.Close();
            var buffer = memoryStream.ToArray();

            return buffer;
        }
    }
}
