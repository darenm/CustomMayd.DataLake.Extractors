//
// Fixed Width Extractor
// From Article by Bryan C Smith
// https://blogs.msdn.microsoft.com/data_otaku/2016/10/27/a-fixed-width-extractor-for-azure-data-lake-analytics/'
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;

namespace CustomMayd.DataLake.Extractors
{
    [SqlUserDefinedExtractor]
    public class StartEndColumnExtractor : IExtractor
    {
        private readonly List<ColumnDefinition> _columnDefinitions;
        private readonly bool _indexStartsAtZero;
        private readonly Encoding _encoding;
        private readonly byte[] _rowDelim;

        public StartEndColumnExtractor(List<ColumnDefinition> columnDefinitions, bool indexStartsAtZero = false, Encoding encoding = null, string rowDelim = "\r\n")
        {
            _columnDefinitions = columnDefinitions;
            _indexStartsAtZero = indexStartsAtZero;
            _encoding = encoding ?? Encoding.UTF8;
            _rowDelim = _encoding.GetBytes(rowDelim);
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            foreach (var currentLine in input.Split(_rowDelim))
                using (var lineReader = new StreamReader(currentLine, _encoding))
                {
                    var line = lineReader.ReadToEnd();

                    //for each column
                    var i = 0;
                    foreach (var columnDefinition in _columnDefinitions)
                    {
                        var startPos = columnDefinition.Start - (_indexStartsAtZero ? 0 : 1);
                        var charsToRead = columnDefinition.End - (columnDefinition.Start - 1);
                        //read chars associated with fixed-width column
                        var value = line.Substring(startPos, charsToRead);


                        //assign value to output (w/ appropriate type)
                        switch (output.Schema[i].Type.Name)
                        {
                            case "String":
                                output.Set(i, value.Trim());
                                break;
                            case "Int32":
                                output.Set(i, int.Parse(value));
                                break;
                            case "Double":
                                output.Set(i, double.Parse(value));
                                break;
                            case "Float":
                                output.Set(i, float.Parse(value));
                                break;
                            case "DateTime":
                                output.Set(i, DateTime.Parse(value));
                                break;
                            default:
                                throw new Exception("Unknown data type specified: " + output.Schema[i].Type.Name);
                        }

                        i++;
                    }

                    //send output
                    yield return output.AsReadOnly();
                }
        }
    }
}