using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomMayd.DataLake.Extractors
{
    public class ColumnDefinition
    {
        public string Name { get; set; }
        public int Start { get; set; }
        public int End { get; set; }
    }
}
