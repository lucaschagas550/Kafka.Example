using Confluent.Kafka;
using System.IO.Compression;
using System.Text.Json;

namespace MessageBus.Extensions
{
    internal class Deserializer<T> : IDeserializer<T>
    {
        //Deserilizar a mensagem kafka
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            //Descompactacao da mensagem
            using var memoryStream = new MemoryStream(data.ToArray());
            using var zip = new GZipStream(memoryStream, CompressionMode.Decompress, true);

            return JsonSerializer.Deserialize<T>(zip);
        }
    }
}
