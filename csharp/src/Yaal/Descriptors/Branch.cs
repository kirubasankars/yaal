// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json.Serialization;
using Yaal.Sql;

namespace Yaal.Descriptors;

public sealed class Branch
{
    public string Name { get; set; } = "";
    public string Method { get; set; } = "";
    public string Path { get; set; } = "";
    [JsonPropertyName("input_type")]
    public string InputType { get; set; } = YaalConst.Object;
    [JsonPropertyName("output_type")]
    public string OutputType { get; set; } = YaalConst.Array;
    [JsonPropertyName("partition_by")]
    public string? PartitionBy { get; set; }
    [JsonPropertyName("use_parent_rows")]
    public bool UseParentRows { get; set; }
    public Dictionary<string, ParamDecl>? Parameters { get; set; }
    public List<Twig>? Twigs { get; set; }
    public List<Branch>? Branches { get; set; }
    public List<string>? Connections { get; set; }
    public DescriptorModel? Model { get; set; }
}

public sealed class DescriptorModel
{
    public Dictionary<string, object?>? Args { get; set; }
    public Dictionary<string, object?>? Payload { get; set; }
    public Dictionary<string, object?>? Output { get; set; }
}
