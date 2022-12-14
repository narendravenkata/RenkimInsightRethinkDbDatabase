using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RethinkDb.Driver;
using RethinkDb.Driver.Net;

namespace RenkimInsightRethinkDbDatabase
{
    public class DatabaseHelper
    {
        public Dictionary<string, List<string>> _tablesIndexes = new Dictionary<string, List<string>>
        {
            {"acs_downloaded_files" , new List<string> {}},
            {"acs_return_files" , new List<string> {"CustomerId"}},
            {"aec_downloaded_files" , new List<string> {}},
            {"aec_input_files", new List<string> {"CustomerId" , "IncomingTransmissionFileId"} },
            {"aec_uploaded_files", new List<string> {} },
            {"audit_transmission_files", new List<string> { "ZipTransmissionFileId" } },
            {"certified_mail_input_files", new List<string> {"CustomerId" , "RenkimId"} },
            {"client_codes" , new List<string> {"ProjectId"}},
            {"common_input_details", new List<string> {"IncomingTransmissionFileId" , "RenkimId"} },
            {"common_input_files" , new List<string> {"CustomerId" , "IncomingTransmissionFileId" , "RenkimId"}},
            {"customer_business_rules" , new List<string> {"ProjectId"}},
            {"customers" , new List<string> {}},
            {"data_transmission_files", new List<string> { "ZipTransmissionFileId" } },
            {"held_incoming_transmission_files" , new List<string> {}},
            {"held_incoming_transmissions", new List<string> {} },
            {"incoming_transmission_files" , new List<string> {}},
            {"inventories" , new List<string> {}},
            {"job_audit" , new List<string> {}},
            {"job_files" , new List<string> {"CustomerId"}},
            {"job_id" , new List<string> {}},
            {"job_id_log", new List<string> {} },
            {"job_run_number" , new List<string> {"CustomerId"}},
            {"letter_code_count_reports" , new List<string> {}},
            {"letters" , new List<string> {"ProjectId"}},
            {"mail_track_input_files", new List<string> {"CustomerId"}},
            {"mail_track_reference_files" , new List<string> {"ImbTrackCode"}},
            {"milestones" , new List<string> {"JobFileId"}},
            {"nyc_zip_codes" , new List<string> {}},
            {"postal_accounting_systems" , new List<string> {}},
            {"presort" , new List<string> {}},
            {"projects", new List<string> { "CustomerId" } },
            {"recurring_schedules" , new List<string> {}},
            {"renkim_360_consent_files" , new List<string> {}},
            {"renkim_360_input_files" , new List<string> {"CustomerId" , "RenkimId"}},
            {"renkim_360_processed_files", new List<string> {} },
            {"renkim_360_return_files", new List<string> {} },
            {"renkim_360_submitted_files", new List<string> {} },
            {"renkim_id", new List<string> {} },
            {"return_mail_input_files", new List<string> { "CustomerId" } },
            {"service_fees", new List<string> { "CustomerId" } },
            {"services", new List<string> { "CustomerId" } },
            {"snd_right_input_files", new List<string> {"CustomerId" , "RenkimId"} },
            {"spoilage_input_files" , new List<string> {"CustomerId" , "IncomingTransmissionFileId" , "RenkimId"}},
            {"summary_reports", new List<string> {"CustomerId" , "JobFileId"} },
            {"unique_common_input_details", new List<string> {} },
            {"unique_common_input_files" , new List<string> {}},
            {"users" , new List<string> {}},
            {"watch_list_input_files", new List<string> {"CustomerId" , "WatchlistId"} },
            {"watch_lists" , new List<string> {}},
            {"work_order_number", new List<string> {} },
            {"zip_transmission_files", new List<string> {"CustomerId"}}

        };

        private readonly IConnection _connection;

        public DatabaseHelper()
        {
            _connection = RethinkDB.R.Connection()
                .Hostname(ConfigurationManager.AppSettings["rethinkServerHostName"])
                .Port(RethinkDBConstants.DefaultPort).Connect();
        }
        public bool CreateTablesIndexes()
        {
            var databaseName = "nex_gen";


            var tables = RethinkDB.R.Db("nex_gen").TableList().RunAtom<string[]>(_connection).ToList();

            foreach (var tableName in _tablesIndexes.Keys)
            {
                if (!tables.Contains(tableName))
                {
                    RethinkDB.R.Db(databaseName).TableCreate(tableName).Run(_connection);

                }

                var requiredIndexes = _tablesIndexes[tableName];

                if (requiredIndexes.Count > 0)
                {
                    foreach (var index in requiredIndexes)
                    {
                        var indexes = RethinkDB.R.Db(databaseName).Table(tableName).IndexList().RunAtom<string[]>(_connection).ToList();
                        if (!indexes.Contains(index))
                        {
                            RethinkDB.R.Db(databaseName).Table(tableName).IndexCreate(index).Run(_connection);
                        }
                        
                    }
                    
                }

            

                //if (!indexes.Contains("RenkimId"))
                //{
                //    RethinkDB.R.Db(databaseName).Table(tableName).IndexCreate("RenkimId").Run(conn);
                //}

            }

            return true;
        }
    }
}
