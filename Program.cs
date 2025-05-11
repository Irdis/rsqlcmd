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
        var command = connection.CreateCommand();
        command.CommandText = GetCommandText(args);
        await connection.OpenAsync();
        await using var reader = await command.ExecuteReaderAsync();
        while (true)
        {
            PrintHeader(reader);
            while (await reader.ReadAsync())
            {
                PrintRow(reader, args);
            }
            if (!(await reader.NextResultAsync()))
                break;
        }
    }

    private static string GetCommandText(RSqlCmdArgs args)
    {
        if (args.File != null)
        {
            return File.ReadAllText(args.File);
        }
        return args.Script;
    }

    private static void PrintHeader(SqlDataReader reader)
    {
        Console.Write("# ");
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

    private static void PrintRow(SqlDataReader reader, RSqlCmdArgs args)
    {
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var value = ProcessValue(args, reader.GetValue(i).ToString());
            Console.WriteLine($"{i + 1}. {value} ");
        }
        Console.WriteLine();
    }

    private static string ProcessValue(RSqlCmdArgs args, string value)
    {
        if (args.NoNewLines)
        {
            var index = value.IndexOf("\r\n");
            if (index >= 0)
            {
                return value.Substring(0, index);
            }
        }
        return value;
    }
}
