using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;

namespace MyRainbow
{
	/// <summary>
	/// http://codereview.stackexchange.com/a/140435
	/// </summary>
	static class Extensions
	{
		public static IEnumerable<IEnumerable<T>> CartesianProduct<T>(this IEnumerable<IEnumerable<T>> sources) =>
			sources.Skip(1).Any() ?
				sources.Skip(1).CartesianProduct().SelectMany(cp => sources.First().Select(s => new[] { s }.Concat(cp))) :
				sources.First().Select(c => new[] { c });

		public static IEnumerable<IEnumerable<T>> CartesianProductAsyncParallel<T>(this IEnumerable<IEnumerable<T>> sources) =>
			sources.AsParallel().Skip(1).Any() ?
				sources.AsParallel().Skip(1).CartesianProductAsyncParallel().SelectMany(cp => sources.First().AsParallel().Select(s => new[] { s }.Concat(cp))) :
				sources.First().Select(c => new[] { c });

		public static IEnumerable<IEnumerable<T>> CartesianProductAggregate<T>(this IEnumerable<IEnumerable<T>> sequences)
		{
			IEnumerable<IEnumerable<T>> emptyProduct = new[] { Enumerable.Empty<T>() };

			return sequences.AsParallel().Aggregate(
				emptyProduct,
				(accumulator, sequence) =>
				from accseq in accumulator
				from item in sequence
				select accseq.Concat(new[] { item }));
		}
	}

	/// <summary>
	/// http://stackoverflow.com/a/1833128/4429828
	/// </summary>
	public static class TConverter
	{
		public static T ChangeType<T>(object value)
		{
			return (T)ChangeType(typeof(T), value);
		}

		public static object ChangeType(Type t, object value)
		{
			TypeConverter tc = TypeDescriptor.GetConverter(t);
			return tc.ConvertFrom(value);
		}

		public static void RegisterTypeConverter<T, TC>() where TC : TypeConverter
		{
			TypeDescriptor.AddAttributes(typeof(T), new TypeConverterAttribute(typeof(TC)));
		}
	}
}
