using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RenkimInsightRethinkDbDatabase
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Started Creating Tables for Renkim Insight");

            var helper = new DatabaseHelper();
            if (!helper.CreateTablesIndexes())
            {

            }
        }
    }
}
