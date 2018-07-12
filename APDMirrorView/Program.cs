using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
using System.Xml.Linq;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using NLog;
using System.IO;


// Regular expresion to be used in notepad++
// .*record.*\r?\n
namespace LibraryIndexAnalysis
{
    class Program
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();


        public class LookupRecord
        {
            public string Number;
            public string Name;
            public string StartDate;
            public string EndDate;
            public string DateReported;
            public string Description_Code;
            public string Description_Description;
            public string Address_StreetAddress;
            public string Address_City_Description;
            public string Address_State_Code;
            public string Discriminator;
            public string SubModuleType_Description;
            public string CaseNumber;
        }

        public static LookupRecord lookupRecord = new LookupRecord();

        static void Main(string[] args)
        {
            logger.Trace("Process Started: " + DateTime.Now);
            Console.WriteLine("Process Started: " + DateTime.Now);           

            FeedLookupTable();

            logger.Trace("Process Completed: " + DateTime.Now);
            Console.WriteLine("Process Completed: " + DateTime.Now);
            Console.WriteLine("Press any key to exit.");
            string line = Console.ReadLine();
        }

        static void FeedLookupTable()
        {
            try
            {
                string queryString = "";
                //int recordsCount = 0;
                int recordsAddedCount = 0;
                //int recordsIgnoredCount = 0;
                var remoteConnectionString = ConfigurationManager.ConnectionStrings["RemoteLookupSQL"].ConnectionString;
                var localConnectionString = ConfigurationManager.ConnectionStrings["LocalLookupSQL"].ConnectionString;

                ClearLookupRecord(localConnectionString);

                logger.Trace(" Getting records from  VFR_DIMS View ...");
                Console.WriteLine(" Getting records from  VFR_DIMS View ...");

                // GEt Data from remote server
                queryString = "SELECT Summaries.Number AS SummaryReportNumber, Summaries.Name AS SummaryReportTitle, PublicSafetyEvents.StartDate AS IncidentDate, " +
                                       "PublicSafetyEvents.EndDate, PublicSafetyEvents.DateReported, " +
                                        " PublicSafetyEvents.Description_Code AS CallTypeCode, PublicSafetyEvents.Description_Description AS CallType, " +
                                        " PublicSafetyEvents.Address_StreetAddress AS IncidentLocation, " +
                                        " PublicSafetyEvents.Address_City_Description AS City, PublicSafetyEvents.Address_State_Code AS State,  " +
                                        " Summaries.Discriminator AS ReportType, " +
                                        " Summaries.SubModuleType_Description AS ReportSubType, COALESCE(Cases.Number, ' ') AS CaseNumber " +
                                        " FROM Summaries INNER JOIN PublicSafetyEvents ON Summaries.Id = PublicSafetyEvents.Id  " +
                                        " LEFT OUTER JOIN Cases ON Cases.Id = Summaries.CaseId";

                using (var connection = new SqlConnection(remoteConnectionString))
                {
                    var command = new SqlCommand(queryString, connection);
                    connection.Open();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // This section was removed since there is no unique key in the data that is retrieved from the Database
                            //recordsFound = SummaryNumberExit(String.Format("{0}", reader[0]), localConnectionString); 
                            //// Check if records already exist
                            //if (recordsFound > 1)
                            //{
                            //    recordsIgnoredCount++;
                            //    logger.Trace("      Summary Number: " + String.Format("{0}", reader[0]) + " already exist in the Database.");
                            //    logger.Trace("          Records found: " + recordsFound.ToString());
                            //    Console.WriteLine("      Summary Number: " + String.Format("{0}", reader[0]) + " already exist in the Database.");

                            //    // Get Record Information
                            //    lookupRecord = GetRecordInformation(String.Format("{0}", reader[0]), remoteConnectionString);

                            //    logger.Trace("      Check if there is a change in Database Record...");
                            //    // Perform a comparison , field by field
                            //    if (String.Format("{0}", reader[1]).Trim() != lookupRecord.Name.Trim() ||
                            //        String.Format("{0}", reader[2]).Trim() != lookupRecord.StartDate.Trim() ||
                            //        String.Format("{0}", reader[3]).Trim() != lookupRecord.EndDate.Trim() ||
                            //        String.Format("{0}", reader[4]).Trim() != lookupRecord.DateReported.Trim() ||
                            //        String.Format("{0}", reader[5]).Trim() != lookupRecord.Description_Code.Trim() ||
                            //        String.Format("{0}", reader[6]).Trim() != lookupRecord.Description_Description.Trim() ||
                            //        String.Format("{0}", reader[7]).Trim() != lookupRecord.Address_StreetAddress.Trim() ||
                            //        String.Format("{0}", reader[8]).Trim() != lookupRecord.Address_City_Description.Trim() ||
                            //        String.Format("{0}", reader[9]).Trim() != lookupRecord.Address_State_Code.Trim() ||
                            //        String.Format("{0}", reader[10]).Trim() != lookupRecord.Discriminator.Trim() ||
                            //        String.Format("{0}", reader[11]).Trim() != lookupRecord.SubModuleType_Description.Trim() ||
                            //        String.Format("{0}", reader[12]).Trim() != lookupRecord.CaseNumber.Trim())
                            //    {
                            //        // Someting change so we ned to update the records in the VFRLookup Table
                            //        logger.Trace("      There has been a change in the Record Information so an Update of the record in VFRLookup is required.");
                            //        Console.WriteLine("      There has been a change in the Record Information so an Update of the record in VFRLookup is required.");
                            //        // Perform and Update ...
                            //        logger.Trace("              Name");
                            //        logger.Trace("                  " + String.Format("{0}", reader[1]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Name.Trim());
                            //        logger.Trace("              Start Date");
                            //        logger.Trace("                  " + String.Format("{0}", reader[2]).Trim());
                            //        logger.Trace("                  " + lookupRecord.StartDate.Trim());
                            //        logger.Trace("              End Date");
                            //        logger.Trace("                  " + String.Format("{0}", reader[3]).Trim());
                            //        logger.Trace("                  " + lookupRecord.EndDate.Trim());
                            //        logger.Trace("              Date Reported");
                            //        logger.Trace("                  " + String.Format("{0}", reader[4]).Trim());
                            //        logger.Trace("                  " + lookupRecord.DateReported.Trim());
                            //        logger.Trace("              Description Code");
                            //        logger.Trace("                  " + String.Format("{0}", reader[5]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Description_Code.Trim());
                            //        logger.Trace("              Descriptio Description");
                            //        logger.Trace("                  " + String.Format("{0}", reader[6]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Description_Description.Trim());
                            //        logger.Trace("              Address Street");
                            //        logger.Trace("                  " + String.Format("{0}", reader[7]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Address_StreetAddress.Trim());
                            //        logger.Trace("              Address City Description");
                            //        logger.Trace("                  " + String.Format("{0}", reader[8]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Address_City_Description.Trim());
                            //        logger.Trace("              Address State Code");
                            //        logger.Trace("                  " + String.Format("{0}", reader[9]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Address_State_Code.Trim());
                            //        logger.Trace("              Discriminator");
                            //        logger.Trace("                  " + String.Format("{0}", reader[10]).Trim());
                            //        logger.Trace("                  " + lookupRecord.Discriminator.Trim());
                            //        logger.Trace("              SubModuleType Description");
                            //        logger.Trace("                  " + String.Format("{0}", reader[11]).Trim());
                            //        logger.Trace("                  " + lookupRecord.SubModuleType_Description.Trim());
                            //        logger.Trace("              CaseNumber");
                            //        logger.Trace("                  " + String.Format("{0}", reader[12]).Trim());
                            //        logger.Trace("                  " + lookupRecord.CaseNumber.Trim());


                            //        UpdateLookupRecord(lookupRecord.Number, lookupRecord.Name, lookupRecord.StartDate, lookupRecord.EndDate , lookupRecord.DateReported,
                            //                           lookupRecord.Description_Code, lookupRecord.Description_Description, lookupRecord.Address_StreetAddress,
                            //                           lookupRecord.Address_City_Description, lookupRecord.Address_State_Code, lookupRecord.Discriminator,
                            //                           lookupRecord.SubModuleType_Description, lookupRecord.CaseNumber, localConnectionString);
                            //    }
                            //    else
                            //    {
                            //        logger.Trace("      No changes detected in Record Information.");
                            //        Console.WriteLine("      No changes detected in Record Information.");
                            //    }
                            //}
                            //else
                            //{
                            //    recordsAddedCount++;
                            //    logger.Trace("      Adding Summary Number: " + String.Format("{0}", reader[0]) + " to VFRLookup Database Table.");
                            //    Console.WriteLine("      Adding Summary Number: " + String.Format("{0}", reader[0]) + " to VFRLookup Database Table.");
                            //    // Perform an Insert ...
                            //    AddLookupRecord(String.Format("{0}", reader[0]), String.Format("{0}", reader[1]), String.Format("{0}", reader[2]),
                            //                    String.Format("{0}", reader[3]), String.Format("{0}", reader[4]), String.Format("{0}", reader[5]),
                            //                    String.Format("{0}", reader[6]), String.Format("{0}", reader[7]), String.Format("{0}", reader[8]),
                            //                    String.Format("{0}", reader[9]), String.Format("{0}", reader[10]), String.Format("{0}", reader[11]),
                            //                    String.Format("{0}", reader[12]), localConnectionString);
                            //}

                            recordsAddedCount++;
                            logger.Trace("      Adding Summary Number: " + String.Format("{0}", reader[0]) + " to VFRLookup Database Table.");
                            Console.WriteLine("      Adding Summary Number: " + String.Format("{0}", reader[0]) + " to VFRLookup Database Table.");
                            // Perform an Insert ...
                            AddLookupRecord(String.Format("{0}", reader[0]), String.Format("{0}", reader[1]), String.Format("{0}", reader[2]),
                                            String.Format("{0}", reader[3]), String.Format("{0}", reader[4]), String.Format("{0}", reader[5]),
                                            String.Format("{0}", reader[6]), String.Format("{0}", reader[7]), String.Format("{0}", reader[8]),
                                            String.Format("{0}", reader[9]), String.Format("{0}", reader[10]), String.Format("{0}", reader[11]),
                                            String.Format("{0}", reader[12]), localConnectionString);

                            //recordsCount++;
                            //Console.WriteLine("     Number: " + String.Format("{0}", reader[0]));
                            //Console.WriteLine("     Name: " + String.Format("{0}", reader[1]));
                            //Console.WriteLine("     Start Date: " + String.Format("{0}", reader[2]));
                            //Console.WriteLine("     End Date: " + String.Format("{0}", reader[3]));
                            //Console.WriteLine("     Date Reported: " + String.Format("{0}", reader[4]));
                            //Console.WriteLine("     Description Code: " + String.Format("{0}", reader[5]));
                            //Console.WriteLine("     Description: " + String.Format("{0}", reader[6]));
                            //Console.WriteLine("     Street: " + String.Format("{0}", reader[7]));
                            //Console.WriteLine("     City: " + String.Format("{0}", reader[8]));
                            //Console.WriteLine("     State Code: " + String.Format("{0}", reader[9]));
                            //Console.WriteLine("     Descriminator: " + String.Format("{0}", reader[10]));
                            //Console.WriteLine("     Sub Module Description: " + String.Format("{0}", reader[11]));
                            //Console.WriteLine("     Case Number: " + String.Format("{0}", reader[12]));

                        }
                    }
                    //logger.Trace(" Number of Records found: " + recordsCount.ToString());
                    //Console.WriteLine(" Number of Records found: " + recordsCount.ToString());
                    logger.Trace(" Number of Records Added to VFRLookup Database Table: " + recordsAddedCount.ToString());
                    Console.WriteLine(" Number of Records Added to VFRLookup Database Table: " + recordsAddedCount.ToString());
                    //logger.Trace(" Number of Records ignored: " + recordsIgnoredCount.ToString());
                    //Console.WriteLine(" Number of Records ignored: " + recordsIgnoredCount.ToString());
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
                Console.WriteLine("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
            }
            
        }

        public static int SummaryNumberExit(string summaryNumber, string connectionString)
        {
            Boolean result = false;
            int recordsCount = 0;
            try
            {                            

                logger.Trace("      Checking Summary Number: " + summaryNumber + " in the Database.");
                Console.WriteLine("      Cheking Summary Number: " + summaryNumber + " in the Database.");

                string queryString = "SELECT *  FROM dbo.VFRLookup WHERE SummaryReportNumber='" + summaryNumber + "'; ";
                //logger.Error("Query: " + queryString);
                //onsole.WriteLine("Query: " + queryString);

                using (var connection = new SqlConnection(connectionString))
                {
                    var command = new SqlCommand(queryString, connection);
                    //Console.WriteLine("Opening DB Connection ....");
                    connection.Open();
                    //Console.WriteLine("Query run ...");
                    using (var reader = command.ExecuteReader())
                    {
                        //Console.WriteLine("Extacting records from query result ...");
                        while (reader.Read())
                        {
                            //Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                            //Console.WriteLine("     " + String.Format("{0}", reader[0]));
                            recordsCount++;
                        }
                    }
                    connection.Close();
                    //logger.Error("Records Count: " + recordsCount.ToString());
                    //Console.WriteLine("Records Count: " + recordsCount.ToString());
                }
                if (recordsCount != 0)
                {
                    result = true;
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
                Console.WriteLine("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
            }
            return recordsCount;
        }

        public static LookupRecord GetRecordInformation(string summaryNumber, string connectionString)
        {
            LookupRecord result = new LookupRecord();
            try
            {

                logger.Trace("      Getting Record information for : " + summaryNumber);
                Console.WriteLine("      Getting Record information for : " + summaryNumber);

                string  queryString = "SELECT Summaries.Number AS SummaryReportNumber, Summaries.Name AS SummaryReportTitle, PublicSafetyEvents.StartDate AS IncidentDate, " +
                                       "PublicSafetyEvents.EndDate, PublicSafetyEvents.DateReported, " +
                                        " PublicSafetyEvents.Description_Code AS CallTypeCode, PublicSafetyEvents.Description_Description AS CallType, " +
                                        " PublicSafetyEvents.Address_StreetAddress AS IncidentLocation, " +
                                        " PublicSafetyEvents.Address_City_Description AS City, PublicSafetyEvents.Address_State_Code AS State,  " +
                                        " Summaries.Discriminator AS ReportType, " +
                                        " Summaries.SubModuleType_Description AS ReportSubType, COALESCE(Cases.Number, ' ') AS CaseNumber " +
                                        " FROM Summaries INNER JOIN PublicSafetyEvents ON Summaries.Id = PublicSafetyEvents.Id  " +
                                        " LEFT OUTER JOIN Cases ON Cases.Id = Summaries.CaseId " +
                                        " WHERE(Summaries.Number ='" + summaryNumber + "')";

                //logger.Trace("      Qquery : " + queryString);

                //logger.Error("Query: " + queryString);
                //onsole.WriteLine("Query: " + queryString);

                using (var connection = new SqlConnection(connectionString))
                {
                    var command = new SqlCommand(queryString, connection);
                    //Console.WriteLine("Opening DB Connection ....");
                    connection.Open();
                    //Console.WriteLine("Query run ...");
                    using (var reader = command.ExecuteReader())
                    {
                        //Console.WriteLine("Extacting records from query result ...");
                       
                        while (reader.Read())
                        {
                            logger.Trace("              Record found" + String.Format("{0}", reader[0]).Trim());
                            result.Number = String.Format("{0}", reader[0]).Trim();
                            result.Name = String.Format("{0}", reader[1]).Trim();
                            result.StartDate = String.Format("{0}", reader[2]).Trim();
                            result.EndDate = String.Format("{0}", reader[3]).Trim();
                            result.DateReported = String.Format("{0}", reader[4]).Trim();
                            result.Description_Code = String.Format("{0}", reader[5]).Trim();
                            result.Description_Description = String.Format("{0}", reader[6]).Trim();
                            result.Address_StreetAddress = String.Format("{0}", reader[7]).Trim();
                            result.Address_City_Description = String.Format("{0}", reader[8]).Trim();
                            result.Address_State_Code = String.Format("{0}", reader[9]).Trim();
                            result.Discriminator = String.Format("{0}", reader[10]).Trim();
                            result.SubModuleType_Description = String.Format("{0}", reader[11]).Trim();
                            result.CaseNumber = String.Format("{0}", reader[12]).Trim();

                            //Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                            //Console.WriteLine("     " + String.Format("{0}", reader[0]));
                        }
                    }
                    connection.Close();
                    //logger.Error("Records Count: " + recordsCount.ToString());
                    //Console.WriteLine("Records Count: " + recordsCount.ToString());
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
                Console.WriteLine("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
            }
            return result;
        }

        static void AddLookupRecord(string number, string name, string startDate, string endDate, string dateReported, string descriptionCode,
                                    string description, string street, string city, string stateCode, string descriminator, string subModuleDescription, 
                                    string caseNumber, string connectionString)
        {
            try
            {
                int recordsCount = 0;

                name = name.Replace("'", "''");
                descriptionCode = descriptionCode.Replace("'", "''");
                description = description.Replace("'", "''");
                street = street.Replace("'", "''");
                descriminator = descriminator.Replace("'", "''");
                subModuleDescription = subModuleDescription.Replace("'", "''");


                string queryString = "INSERT INTO VFRLookup ( SummaryReportNumber, SummaryReportTitle, IncidentDate, EndDate, DateReported, CallTypeCode, " +
                                     " CallType, IncidentLocation, City, State , " +
                                     " ReportType, ReportSubType, CaseNumber )" +
                                     " VALUES ('" + number.Trim() + "','" + name.Trim() + "','" + startDate.Trim() + "','" + endDate.Trim() + "','" + dateReported.Trim() + "','" + 
                                     descriptionCode.Trim() + "','" +  description.Trim() + "','" + street.Trim() + "','" + city.Trim() + "','" + stateCode.Trim() + "','" + 
                                     descriminator.Trim() + "','" +  subModuleDescription.Trim() + "','" + caseNumber.Trim() + "')";

                logger.Error("Query: " + queryString);
                //Console.WriteLine("Query: " + queryString);

                using (var connection = new SqlConnection(connectionString))
                {
                    var command = new SqlCommand(queryString, connection);
                    //Console.WriteLine("Opening DB Connection ....");
                    connection.Open();
                    //Console.WriteLine("Query run ...");
                    using (var reader = command.ExecuteReader())
                    {
                        //Console.WriteLine("Extactin records from query result ...");
                        while (reader.Read())
                        {
                            //Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                            //Console.WriteLine("     " + String.Format("{0}", reader[0]));
                            recordsCount++;
                        }
                    }
                    connection.Close();
                    //logger.Error("Records Count: " + recordsCount.ToString());
                    //Console.WriteLine("Records Count: " + recordsCount.ToString());
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
                Console.WriteLine("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
            }
        }


        static void UpdateLookupRecord(string number, string name, string startDate, string endDate, string dateReported, string descriptionCode,
                                    string description, string street, string city, string stateCode, string descriminator, string subModuleDescription,
                                    string caseNumber, string connectionString)
        {
            try
            {

                name = name.Replace("'", "''");
                descriptionCode = descriptionCode.Replace("'", "''");
                description = description.Replace("'", "''");
                street = street.Replace("'", "''");
                descriminator = descriminator.Replace("'", "''");
                subModuleDescription = subModuleDescription.Replace("'", "''");

                string queryString = "UPDATE VFRLookup SET SummaryReportTitle ='" +  name.Trim() + "', IncidentDate ='" + startDate.Trim() + "'," +
                                                          "EndDate = '" + endDate.Trim() + "', DateReported = '" + dateReported.Trim() + "'," +
                                                          "CallTypeCode = '" + descriptionCode.Trim() + "', CallType = '" + description.Trim() + "'," +
                                                          "IncidentLocation = '" + street.Trim() + "', City = '" + city.Trim() + "'," +
                                                          "State = '" + stateCode.Trim() + "', ReportType = '" + descriminator.Trim() + "'," +
                                                          "ReportSubType = '" + subModuleDescription.Trim() + "', CaseNumber = '" + caseNumber.Trim() + "'" ;


                //logger.Error("Query: " + queryString);
                //Console.WriteLine("Query: " + queryString);

                using (var connection = new SqlConnection(connectionString))
                {
                    var command = new SqlCommand(queryString, connection);
                    //Console.WriteLine("Opening DB Connection ....");
                    connection.Open();
                    //Console.WriteLine("Query run ...");
                    using (var reader = command.ExecuteReader())
                    {   

                    }
                    connection.Close();
                    //logger.Error("Records Count: " + recordsCount.ToString());
                    //Console.WriteLine("Records Count: " + recordsCount.ToString());
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
                Console.WriteLine("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
            }
        }

        static void ClearLookupRecord(string connectionString)
        {
            try
            {
                string queryString = "DELETE FROM VFRLookup ";

                logger.Error("Removing records from VFRLookup ...");
                Console.WriteLine("Removing records from VFRLookup ...");

                using (var connection = new SqlConnection(connectionString))
                {
                    var command = new SqlCommand(queryString, connection);
                    connection.Open();
                    using (var reader = command.ExecuteReader())
                    {

                    }
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
                Console.WriteLine("Error:" + e.Message + "\n" + "Exception: " + e.InnerException);
            }
        }

    }
}


