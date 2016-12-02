using System;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MyCartesian cart = new MyCartesian(4, "abcdefghijklmopqrstuvwxyz");

            Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
                $"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}");

            var table_of_table_of_chars = cart.Generate();
            var hasher = MD5.Create();
            using (var dbase = new DatabaseHasher())
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
