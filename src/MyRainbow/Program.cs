using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MyRainbow
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MyCartesian cart = new MyCartesian(3, "abcdefg");

            Console.WriteLine($"Hello World! AlphabetPower  = {cart.AlphabetPower}");

            var table_of_table_of_chars = cart.Generate();
            var hasher = MD5.Create();
            foreach (var chars_table in table_of_table_of_chars)
            {
                var str = string.Concat(chars_table);
                var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(str))).Replace("-","").ToLowerInvariant();
                Console.WriteLine($"MD5({str})= {hash}");

                if (Console.KeyAvailable)
                    break;
            }

            Console.ReadKey();
        }
    }
}
