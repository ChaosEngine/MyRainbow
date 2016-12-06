using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MyRainbow
{
    public static class Combiner
    {
        public static IEnumerable<IEnumerable<T>> Combine<T>(IEnumerable<IEnumerable<T>> a, IEnumerable<IEnumerable<T>> b)
        {
            bool found = false;

            foreach (IEnumerable<T> groupa in a.AsParallel())
            {
                found = true;

                foreach (IEnumerable<T> groupB in b.AsParallel())
                    yield return Append(groupa, groupB);
            }

            if (!found)
                foreach (IEnumerable<T> groupB in b)
                    yield return groupB;
        }

        public static IEnumerable<IEnumerable<T>> Combine<T>(IEnumerable<T> a, IEnumerable<IEnumerable<T>> b)
        {
            bool found = false;

            foreach (IEnumerable<T> bGroup in b)
            {
                found = true;
                yield return Append(a, bGroup);
            }

            if (!found)
                yield return a;

        }

        public static IEnumerable<IEnumerable<T>> Combine<T>(IEnumerable<IEnumerable<T>> a, IEnumerable<T> b)
        {
            bool found = false;

            foreach (IEnumerable<T> aGroup in a)
            {
                found = true;
                yield return Append(aGroup, b);
            }

            if (!found)
                yield return b;
        }


        public static IEnumerable<IEnumerable<T>> Combine<T>(T a, IEnumerable<IEnumerable<T>> b)
        {
            bool found = false;

            foreach (IEnumerable<T> bGroup in b)
            {
                found = true;
                yield return Append(a, bGroup);
            }

            if (!found)
                yield return new T[] { a };

        }

        public static IEnumerable<IEnumerable<T>> Combine<T>(IEnumerable<IEnumerable<T>> a, T b)
        {
            bool found = false;

            foreach (IEnumerable<T> aGroup in a)
            {
                found = true;
                yield return Append(aGroup, b);
            }

            if (!found)
                yield return new T[] { b };
        }

        public static IEnumerable<T> Group<T>(T a, T b)
        {
            yield return a;
            yield return b;
        }

        // add the new item at the beginning of the collection
        public static IEnumerable<T> Append<T>(T a, IEnumerable<T> b)
        {
            yield return a;
            foreach (T item in b) yield return item;
        }

        // add the new item at the end of the collection
        public static IEnumerable<T> Append<T>(IEnumerable<T> a, T b)
        {
            foreach (T item in a) yield return item;
            yield return b;
        }

        public static IEnumerable<T> Append<T>(IEnumerable<T> a, IEnumerable<T> b)
        {
            foreach (T item in a) yield return item;
            foreach (T item in b) yield return item;
        }
    }

    public static class CProd
    {
        public static IEnumerable<IEnumerable<T>> Combinations<T>(params IEnumerable<T>[] input)
        {
            IEnumerable<IEnumerable<T>> result = new T[0][];

            foreach (IEnumerable<T> item in input.AsParallel())
                result = Combiner.Combine(result, Combinations(item));

            return result;
        }

        private static IEnumerable<IEnumerable<T>> Combinations<T>(IEnumerable<T> input)
        {
            foreach (T item in input)
                yield return new T[] { item };
        }
    }

    public static class Permuter
    {
        public static IEnumerable<IEnumerable<T>> Permute<T>(IEnumerable<T> input)
        {
            Int32 index = 0;

            foreach (T item in input)
                foreach (IEnumerable<T> grp in Combiner.Combine(item, Permute(Skipper(input, index++))))
                    yield return grp;
        }

        private static IEnumerable<T> Skipper<T>(IEnumerable<T> input, Int32 skipIndex)
        {
            Int32 index = 0;

            foreach (T item in input)
                if (skipIndex != index++)
                    yield return item;
        }

    }

    public static class Chooser
    {
        public static IEnumerable<IEnumerable<T>> Choose<T>(IEnumerable<T> item, Int32 count)
        {
            IEnumerable<IEnumerable<T>> result = new T[0][];

            for (Int32 i = 0; i < count; i++)
                result = Combiner.Combine(result, CProd.Combinations(item));

            return result;
        }
    }
}
