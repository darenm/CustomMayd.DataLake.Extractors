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
    public class FixedWidthExtractor : IExtractor
    {
        private readonly SqlMap<string, string> _colWidths;
        private readonly Encoding _encoding;
        private readonly byte[] _rowDelim;

        public FixedWidthExtractor(SqlMap<string, string> colWidths, Encoding encoding = null, string rowDelim = "\r\n")
        {
            _encoding = encoding ?? Encoding.UTF8;
            _rowDelim = _encoding.GetBytes(rowDelim);
            _colWidths = colWidths;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            foreach (var currentLine in input.Split(_rowDelim))
                using (var lineReader = new StreamReader(currentLine, _encoding))
                {
                    var line = lineReader.ReadToEnd();
                    //read new line of input
                    var startParse = 0;

                    //for each column
                    var i = 0;
                    foreach (var colWidth in _colWidths)
                    {
                        //read chars associated with fixed-width column
                        var charsToRead = int.Parse(colWidth.Value);
                        var value = line.Substring(startParse, charsToRead);


                        //assign value to output (w/ appropriate type)
                        switch (output.Schema[i].Type.Name)
                        {
                            case "String":
                                output.Set(i, value);
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

                        //move to start of next column
                        startParse += charsToRead;
                        i++;
                    }

                    //send output
                    yield return output.AsReadOnly();
                }
        }
    }
}