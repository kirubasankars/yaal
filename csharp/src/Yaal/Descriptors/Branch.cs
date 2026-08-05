using Json.Schema;
using Yaal.Sql;

namespace Yaal.Descriptors;

public sealed class Branch
{
    public string Name { get; set; } = "";
    public string Method { get; set; } = "";
    public string Path { get; set; } = "";
    public string InputType { get; set; } = YaalConst.Object;
    public string OutputType { get; set; } = YaalConst.Array;
    public string? PartitionBy { get; set; }
    public bool UseParentRows { get; set; }
    public bool Cache { get; set; }
    public Dictionary<string, ParamDecl>? Parameters { get; set; }
    public List<Twig>? Twigs { get; set; }
    public List<Branch>? Branches { get; set; }
    public List<string>? Connections { get; set; }
    public DescriptorModel? Model { get; set; }
    public Dictionary<string, JsonSchema?>? Validators { get; set; }
}

public sealed class DescriptorModel
{
    public Dictionary<string, object?>? Args { get; set; }
    public Dictionary<string, object?>? Payload { get; set; }
    public Dictionary<string, object?>? Output { get; set; }
}
