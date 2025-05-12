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
}

public class Program
{
    public static Regex _go = new Regex(@"^\s*GO\s*;*\s*(--.*)?(\\\*.*)?$", RegexOptions.IgnoreCase);
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
            while (true)
            {
                var rowIndex = 1;
                var hasRows = await reader.ReadAsync();
                if (hasRows)
                {
                    PrintHeader(reader);
                    PrintRow(rowIndex++, reader, args);
                }
                while (await reader.ReadAsync())
                {
                    PrintRow(rowIndex++, reader, args);
                }
                if (!(await reader.NextResultAsync()))
                    break;
            }
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
        foreach(var line in lines)
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

    private static void PrintHeader(SqlDataReader reader)
    {
        Console.Write("Table cols: ");
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            if (string.IsNullOrEmpty(name))
            {
                name = "<no name>";
            }
            Console.Write($"{i + 1}) {name} ");
        }
        Console.WriteLine();
        Console.WriteLine();
    }

    private static void PrintRow(int rowIndex, SqlDataReader reader, RSqlCmdArgs args)
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
        var str = value.ToString();
        if (args.NoNewLines)
        {
            var index = str.IndexOf("\r\n");
            if (index >= 0)
            {
                return str.Substring(0, index);
            }
        }
        return str;
    }
}
