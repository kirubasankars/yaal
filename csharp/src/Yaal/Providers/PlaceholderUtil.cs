namespace Yaal.Providers;

public static class PlaceholderUtil
{
    public static (string Content, List<string> Names) ToNumbered(
        string sqlContent, int argCount, Func<int, string> nameForIndex)
    {
        var parts = sqlContent.Split("%s");
        if (parts.Length == 1)
            return (sqlContent, new List<string>());

        if (parts.Length - 1 != argCount)
        {
            throw new ArgumentException(
                $"Bind count mismatch: {parts.Length - 1} placeholders, {argCount} values");
        }

        var names = new List<string>();
        var rendered = parts[0];
        for (var i = 0; i < argCount; i++)
        {
            var name = nameForIndex(i);
            names.Add(name);
            rendered += name + parts[i + 1];
        }
        return (rendered, names);
    }
}
