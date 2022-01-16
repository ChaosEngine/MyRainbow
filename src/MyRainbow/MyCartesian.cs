using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MyRainbow
{
	public class ThinHashes
	{
		//[JsonProperty(PropertyName = "Key")]
		[BsonId]
		public string Key { get; set; }

		//[JsonProperty(PropertyName = "HashMD5")]
		public string HashMD5 { get; set; }

		//[JsonProperty(PropertyName = "HashSHA256")]
		public string HashSHA256 { get; set; }
	}

	class MyCartesian
	{
		//private const string _longAlphabet = @"abcdefghijklmnopqrstuvwxyz0123456789";
		private const string _smallAlphabet = @"abcd";

		public string Alphabet { get; private set; }

		public int Length { get; private set; }

		public int AlphabetPower { get { return Alphabet.Length; } }

		public double CombinationCount
		{
			get { return Math.Pow(AlphabetPower, Length); }
		}

		public MyCartesian(int length = 4, string alphabet = null)
		{
			Length = length;

			if (!string.IsNullOrEmpty(alphabet))
				Alphabet = alphabet;
			else
				Alphabet = _smallAlphabet;
			Alphabet = string.Concat(Alphabet.Distinct());

			if (Length > Alphabet.Length)
				throw new IndexOutOfRangeException("alphabet has not enough characters");
		}

		public IEnumerable<IEnumerable<char>> Generate()
		{
			var input = new string[Length];
			for (int i = 0; i < Length; i++)
				input[i] = Alphabet;

			var result = input.CartesianProductAsyncParallel();

			return result;
		}

		public IEnumerable<IEnumerable<char>> Generate2()
		{
			var input = new string[Length];
			for (int i = 0; i < Length; i++)
				input[i] = Alphabet;

			var result = input.CartesianProductAggregate();

			return result;
		}
	}
}
