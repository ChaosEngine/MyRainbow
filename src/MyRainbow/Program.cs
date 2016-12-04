using System;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.UserSecrets;

[assembly: UserSecretsId("aspnet-MyRainbow-20161203120550")]

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
            MyCartesian cart = new MyCartesian(3, "abcdefghijklmopqrstuvwxyz");

            Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
                $"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}");

            var table_of_table_of_chars = cart.Generate();
            var hasher = MD5.Create();
            using (var dbase = new DatabaseHasher(GetConnectionStringFromSecret(args)))
            {
                dbase.EnsureExist();
                dbase.Purge();

                foreach (var chars_table in table_of_table_of_chars)
                {
                    var value = string.Concat(chars_table);
                    var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-", "").ToLowerInvariant();

                    //Console.WriteLine($"MD5({value}) = {hash}");
                    dbase.Insert(value, hash);

                    if (Console.KeyAvailable)
                        break;
                }
            }

            Console.WriteLine("Done");
            Console.ReadKey();
        }
    }
}
