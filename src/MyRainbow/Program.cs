using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MyRainbow
{
    public class Program
    {
        static string GetConnectionStringFromSecret(string[] args)
        {
            var builder = new ConfigurationBuilder()
              // For more details on using the user secret store see http://go.microsoft.com/fwlink/?LinkID=532709
              .AddUserSecrets()
              .AddEnvironmentVariables()
              .AddCommandLine(args);
            var configuration = builder.Build();

            string conn_str = configuration["SqlConnection"];
            return conn_str;
        }

        public static void Main(string[] args)
        {
            MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

            Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
                $"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
                $"Combination count = {cart.CombinationCount}");

            var table_of_table_of_chars = cart.Generate2();
            Console.WriteLine("Keys generated");
            var hasher = MD5.Create();
            var stopwatch = new Stopwatch();
            using (var dbase = new DatabaseHasher(GetConnectionStringFromSecret(args)))
            {
                //dbase.EnsureExist();
                //dbase.Purge();

                stopwatch.Start();
                long counter = 0;
                foreach (var chars_table in table_of_table_of_chars)
                {
                    var value = string.Concat(chars_table);
                    var hash = "aaa";// BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-", "").ToLowerInvariant();

                    if (counter % 10000 == 0)
                        Console.WriteLine($"MD5({value}) = {hash}, counter = {counter}");
                    //dbase.Insert(value, hash);

                    if (Console.KeyAvailable)
                        break;
                    counter++;
                }
                /*Parallel.ForEach(table_of_table_of_chars, (chars_table, parallelLoopState, counter) =>
                {
                    if (parallelLoopState.IsStopped || parallelLoopState.IsExceptional || parallelLoopState.ShouldExitCurrentIteration)
                        return;

                    var value = string.Concat(chars_table);
                    var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-", "").ToLowerInvariant();

                    if (counter % 10000 == 0)
                        Console.WriteLine($"MD5({value}) = {hash}, counter = {counter}");
                    //dbase.Insert(value, hash);

                    if (Console.KeyAvailable)
                        parallelLoopState.Stop();
                });*/
                stopwatch.Stop();
            }

            Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
            Console.ReadKey();
        }
    }
}
