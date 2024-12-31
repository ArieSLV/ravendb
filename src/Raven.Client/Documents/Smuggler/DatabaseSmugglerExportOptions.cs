using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
        public ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }
        
        public override DynamicJsonValue ToAuditJson()
        {
            var json = base.ToAuditJson();
            json[nameof(CompressionAlgorithm)] = CompressionAlgorithm;
            return json;
        }
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
        ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }
    }
}
