using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyRainbow
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MyCartesian cart = new MyCartesian(10);

            Console.WriteLine($"Hello World! AlphabetPower  = {cart.AlphabetPower}");

            var table_of_table_of_chars = cart.Generate();
            foreach (var chars_table in table_of_table_of_chars)
            {
                Console.WriteLine(string.Concat(chars_table));

                if (Console.KeyAvailable)
                    break;
            }

            Console.ReadKey();
        }
    }
}
