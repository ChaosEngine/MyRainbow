using System.Collections.Generic;
using System.Linq;

namespace MyRainbow
{
    class MyCartesian
    {
        private const string _longAlphabet = @"abcdefghijklmnopqrstuvwxyz0123456789";
        private const string _smallAlphabet = @"abcd";

        public string Alphabet { get; private set; }

        public int Length { get; private set; }

        public int AlphabetPower { get { return Alphabet.Length; } }

        public MyCartesian(int length = 4, string alphabet = null)
        {
            Length = length;

            if (!string.IsNullOrEmpty(alphabet))
                Alphabet = alphabet;
            else
                Alphabet = _smallAlphabet;
            Alphabet = string.Concat(Alphabet.Distinct());
        }

        public IEnumerable<IEnumerable<char>> Generate()
        {
            var input = new string[Length];
            for (int i = 0; i < Length; i++)
                input[i] = Alphabet;

            var result = input.CartesianProductAsync();

            return result;
        }
    }

    /// <summary>
    /// http://codereview.stackexchange.com/a/140435
    /// </summary>
    static class CartesianProducts
    {

        public static IEnumerable<IEnumerable<T>> CartesianProduct<T>(this IEnumerable<IEnumerable<T>> sources) =>
            sources.Skip(1).Any() ?
                sources.Skip(1).CartesianProductAsync().SelectMany(cp => sources.First().Select(s => new[] { s }.Concat(cp))) :
                sources.First().Select(c => new[] { c });

        public static IEnumerable<IEnumerable<T>> CartesianProductAsync<T>(this IEnumerable<IEnumerable<T>> sources) =>
            sources.AsParallel().Skip(1).Any() ?
                sources.AsParallel().Skip(1).CartesianProductAsync().SelectMany(cp => sources.First().AsParallel().Select(s => new[] { s }.Concat(cp))) :
                sources.First().Select(c => new[] { c });
    }
}