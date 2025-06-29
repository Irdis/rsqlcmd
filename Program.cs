using System.Data;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace rsqlcmd;

public class RSqlCmdArgs
{
    public string ConnectionString { get; set; }
    public string File { get; set; }
    public string Script { get; set; }
    public bool NoNewLines { get; set; }
    public bool InsertMode { get; set; }
}

public class Program
{
    public static Regex _go = new Regex(@"^\s*GO\s*;*\s*(--.*)?(\\\*.*)?$", RegexOptions.IgnoreCase);
    private const string NoNameColumn = "NoName{0}";
    private static readonly int NewLineLength = Environment.NewLine.Length;

    public static async Task Main(string[] args)
    {
        if (!ParseArgs(args, out var sqlCmdArgs))
        {
            return;
        }
        await ExecuteSql(sqlCmdArgs);
    }

    private static bool ParseArgs(string[] args, out RSqlCmdArgs res)
    {
        res = new RSqlCmdArgs();
        var ind = 0;
        while (ind < args.Length)
        {
            var arg = args[ind];
            if (arg == "-c" && ind < args.Length - 1)
            {
                ind++;
                res.ConnectionString = args[ind];
                ind++;
            }
            else if (arg == "-f" && ind < args.Length - 1)
            {
                ind++;
                res.File = args[ind];
                ind++;
            }
            else if (arg == "-s" && ind < args.Length - 1)
            {
                ind++;
                res.Script = args[ind];
                ind++;
            }
            else if (arg == "-nnl")
            {
                ind++;
                res.NoNewLines = true;
            }
            else if (arg == "-i")
            {
                ind++;
                res.InsertMode = true;
            }
            else if (arg == "-?")
            {
                PrintUsage(Console.Out);
                return false;
            }
            else
            {
                PrintInvalidArgs();
                return false;
            }
        }
        if (res.File == null && res.Script == null)
        {
            PrintInvalidArgs();
            return false;
        }
        return true;
    }

    private static void PrintInvalidArgs()
    {
        var tw = Console.Error;
        tw.WriteLine("Invalid argument list");
        tw.WriteLine();
        PrintUsage(tw);
    }

    private static void PrintUsage(TextWriter tw)
    {
        tw.WriteLine("rsqlcmd [args]:");
        tw.WriteLine("    -c <conn>:   connection string");
        tw.WriteLine("    -f <file>:   file path");
        tw.WriteLine("    -s <script>: raw script");
        tw.WriteLine("    -nnl:        no new lines");
        tw.WriteLine("    -i:          generate inserts");
        tw.WriteLine("    -?:          show this message");
    }

    private static async Task ExecuteSql(RSqlCmdArgs args)
    {
        await using var connection = new SqlConnection(args.ConnectionString);
        connection.InfoMessage += (sender, e) =>
        {
            Console.WriteLine(e.Message);
        };
        await connection.OpenAsync();
        foreach (var commandText in GetCommandText(args))
        {
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            await using var reader = await command.ExecuteReaderAsync();
            var tableIndex = 1;
            while (true)
            {
                await PrintTable(reader, args, tableIndex);
                if (!(await reader.NextResultAsync()))
                    break;
                tableIndex++;
            }
        }
    }

    private static Task PrintTable(SqlDataReader reader, RSqlCmdArgs args, int tableIndex)
    {
        if (args.InsertMode)
        {
            return PrintTableInserts(reader, args, tableIndex);
        }
        return PrintTableText(reader, args);
    }

    private static async Task PrintTableInserts(SqlDataReader reader, RSqlCmdArgs args, int tableIndex)
    {
        var prefix = GenerateInsertPrefix(reader, tableIndex);
        if (prefix == null)
        {
            Console.WriteLine($"-- table{tableIndex} empty");
            Console.WriteLine();
            return;
        }

        PrintTableDefinition(reader, tableIndex);
        await PrintTableContent(reader, tableIndex, prefix, args);
    }

    private static async Task PrintTableContent(SqlDataReader reader, int tableIndex, string prefix, RSqlCmdArgs args)
    {
        const int maxRowsCount = 100;
        var rowIndex = 0;

        var sb = new StringBuilder();
        sb.AppendLine(prefix);
        while (await reader.ReadAsync())
        {
            PrintInsertRow(rowIndex++, reader, args, sb);
            if (rowIndex == maxRowsCount)
            {
                Console.WriteLine(sb.ToString());
                sb.Clear();
                sb.AppendLine(prefix);
                rowIndex = 0;
                continue;
            }
            sb.AppendLine(",");
        }
        if (rowIndex > 0)
        {
            var trimCount = NewLineLength + 1;
            sb.Remove(sb.Length - trimCount, trimCount);
            Console.WriteLine(sb.ToString());
        }
        Console.WriteLine();
    }

    private static void PrintTableDefinition(SqlDataReader reader, int tableIndex)
    {
        var sb = new StringBuilder();
        sb.AppendFormat("CREATE TABLE #table{0} (", tableIndex);
        sb.AppendLine();
        var schemaTable = reader.GetSchemaTable();
        var missingNameCounter = 1;

        for (int i = 0; i < reader.FieldCount; i++)
        {
            var row = schemaTable.Rows[i];
            var name = (string)row["ColumnName"];
            var dataType = (string)row["DataTypeName"].ToString();
            var columnSize = (int)row["ColumnSize"];
            sb.Append("    [");
            sb.Append(GetColumnName(name, ref missingNameCounter));
            sb.Append("] ");
            sb.Append(dataType);
            if (dataType == "varchar" ||
                dataType == "nvarchar")
            {
                sb.Append("(");
                if (columnSize == int.MaxValue)
                    sb.Append("max");
                else
                    sb.Append(columnSize);
                sb.Append(")");
            }
            if (dataType == "decimal")
            {
                var numericPrecision = (Int16)row["NumericPrecision"];
                var numericScale = (Int16)row["NumericScale"];
                sb.AppendFormat("({0}, {1})",  numericPrecision, numericScale);
            }
            sb.AppendLine(",");
        }
        sb.AppendLine(")");
        Console.WriteLine(sb.ToString());
    }

    private static string GenerateInsertPrefix(SqlDataReader reader, int tableIndex)
    {
        if (reader.FieldCount == 0)
            return null;

        var prefix = new StringBuilder();
        prefix.AppendFormat("INSERT INTO #table{0} (", tableIndex);

        var missingNameCounter = 1;
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = GetColumnName(reader.GetName(i), ref missingNameCounter);
            prefix.AppendFormat("[{0}], ", name);
        }
        prefix.Remove(prefix.Length - 2, 2);
        prefix.Append(") VALUES");
        return prefix.ToString();
    }

    private static void PrintInsertRow(int rowIndex, SqlDataReader reader, RSqlCmdArgs args, StringBuilder sb)
    {
        sb.Append("(");
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var val = reader.GetValue(i);
            if (val == DBNull.Value)
            {
                sb.AppendFormat("NULL");
            }
            else
            {
                var str = ProcessValue(args, val);
                sb.AppendFormat("'{0}'", str);
            }

            sb.Append(",");
        }
        if (sb[sb.Length - 1] == ',')
        {
            sb.Remove(sb.Length - 1, 1);
        }
        sb.Append(")");
    }

    private static async Task PrintTableText(SqlDataReader reader, RSqlCmdArgs args)
    {
        var rowIndex = 1;
        PrintTextHeader(reader);
        while (await reader.ReadAsync())
        {
            PrintTextRow(rowIndex++, reader, args);
        }
    }

    private static IEnumerable<string> GetCommandText(RSqlCmdArgs args)
    {
        if (args.File != null)
        {
            return ExtractScripts(File.ReadAllText(args.File));
        }
        return ExtractScripts(args.Script);
    }

    private static IEnumerable<string> ExtractScripts(string text)
    {
        var lines = text.Split(new []{'\r', '\n'}, StringSplitOptions.RemoveEmptyEntries);
        var sb = new StringBuilder();
        foreach (var line in lines)
        {
           if (_go.Match(line).Success)
           {
               if (sb.Length > 0)
               {
                   yield return sb.ToString();
                   sb.Clear();
               }
           }
           else
           {
               sb.AppendLine(line);
           }
        }
        if (sb.Length > 0)
        {
            yield return sb.ToString();
        }
    }

    private static void PrintTextHeader(SqlDataReader reader)
    {
        if (reader.FieldCount == 0)
            return;
        Console.Write("Table cols: ");
        var missingNameCounter = 1;
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = GetColumnName(reader.GetName(i), ref missingNameCounter);
            Console.Write($"{i + 1}) {name} ");
        }
        Console.WriteLine();
        Console.WriteLine();
    }

    private static void PrintTextRow(int rowIndex, SqlDataReader reader, RSqlCmdArgs args)
    {
        Console.WriteLine($"Row index #{rowIndex}");
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var val = reader.GetValue(i);
            var str = ProcessValue(args, val);
            Console.WriteLine($"{i + 1}. {str}");
        }
        Console.WriteLine();
    }

    private static string ProcessValue(RSqlCmdArgs args, object value)
    {
        if (value == DBNull.Value)
            return "<NULL>";
        var str = GetObjectStrValue(value);
        if (args.NoNewLines)
        {
            var newLineIndex = str.IndexOf("\r\n");
            if (newLineIndex >= 0)
            {
                str = str.Substring(0, newLineIndex);
            }
        }
        var zeroIndex = str.IndexOf('\0');
        if (zeroIndex >= 0)
        {
            str = str.Substring(0, zeroIndex);
        }
        return str;
    }

    private static string GetObjectStrValue(object value)
    {
        if (value is int intVal)
            return intVal.ToString(CultureInfo.InvariantCulture);

        if (value is decimal decimalVal)
            return decimalVal.ToString(CultureInfo.InvariantCulture);

        return value.ToString();
    }

    private static string GetColumnName(string columnName, ref int missingNameCounter)
    {
        return string.IsNullOrEmpty(columnName) ? GetNoNameColumn(missingNameCounter++) : columnName;
    }

    private static string GetNoNameColumn(int missingNameCounter)
    {
        return string.Format(NoNameColumn, missingNameCounter);
    }
}
