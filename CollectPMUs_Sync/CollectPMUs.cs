using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Diagnostics;
using TVA.Collections;
using TVA.Configuration;
using TVA.Historian;
using TVA.Historian.Files;
using TVA.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.IO;
using Amazon.S3.Transfer;

namespace CollectPMUs
{
    class CollectPMUs
    {
        public class MetadataWrapper
        {
            private MetadataRecord m_metadata;

            /// <summary>
            /// Creates a new instance of the <see cref="MetadataWrapper"/> class.
            /// </summary>
            /// <param name="metadata">The <see cref="MetadataRecord"/> to be wrapped.</param>
            public MetadataWrapper(MetadataRecord metadata)
            {
                m_metadata = metadata;
            }

            /// <summary>
            /// Determines whether the measurement represented by this metadata record
            /// should be exported to CSV by the Historian Data Viewer.
            /// </summary>
            public bool Export
            {
                get;
                set;
            }

            /// <summary>
            /// Determines whether the measurement represented by this metadata record
            /// should be displayed on the Historian Data Viewer graph.
            /// </summary>
            public bool Display
            {
                get;
                set;
            }
            public bool Data
            {
                get;
                set;
            }
            /// <summary>
            /// Gets the point number of the measurement.
            /// </summary>
            public int PointNumber
            {
                get
                {
                    return m_metadata.HistorianID;
                }
            }

            /// <summary>
            /// Gets the name of the measurement.
            /// </summary>
            public string Name
            {
                get
                {
                    return m_metadata.Name;
                }
            }

            /// <summary>
            /// Gets the description of the measurement.
            /// </summary>
            public string Description
            {
                get
                {
                    return m_metadata.Description;
                }
            }

            /// <summary>
            /// Gets the first alternate name for the measurement.
            /// </summary>
            public string Synonym1
            {
                get
                {
                    return m_metadata.Synonym1;
                }
            }

            /// <summary>
            /// Gets the second alternate name for the measurement.
            /// </summary>
            public string Synonym2
            {
                get
                {
                    return m_metadata.Synonym2;
                }
            }

            /// <summary>
            /// Gets the third alternate name for the measurement.
            /// </summary>
            public string Synonym3
            {
                get
                {
                    return m_metadata.Synonym3;
                }
            }

            /// <summary>
            /// Gets the system name.
            /// </summary>
            public string System
            {
                get
                {
                    return m_metadata.SystemName;
                }
            }

            /// <summary>
            /// Gets the low range of the measurement.
            /// </summary>
            public Single LowRange
            {
                get
                {
                    return m_metadata.Summary.LowRange;
                }
            }

            /// <summary>
            /// Gets the high range of the measurement.
            /// </summary>
            public Single HighRange
            {
                get
                {
                    return m_metadata.Summary.HighRange;
                }
            }

            /// <summary>
            /// Gets the engineering units used to measure the values.
            /// </summary>
            public string EngineeringUnits
            {
                get
                {
                    return m_metadata.AnalogFields.EngineeringUnits;
                }
            }

            /// <summary>
            /// Gets the unit number of the measurement.
            /// </summary>
            public int Unit
            {
                get
                {
                    return m_metadata.UnitNumber;
                }
            }

            /// <summary>
            /// Returns the wrapped <see cref="MetadataRecord"/>.
            /// </summary>
            /// <returns>The wrapped metadata record.</returns>
            public MetadataRecord GetMetadata()
            {
                return m_metadata;
            }
        }
        public static ICollection<ArchiveFile> m_archiveFiles;
        public static List<MetadataWrapper> m_metadata;

        private class pdcServiceCaller
        {
            public string sInputPMUs = "";
            public DateTime dtInitDateTime;
            public DateTime dtEndDateTime;
            public bool bEndReported = false;
            public int iTimeoutInSecs = 300;
            // private string responseFromServer = "";
            // public string sReturn = "";
            // private StringBuilder responseFromServer = new StringBuilder();
            public StringBuilder sReturn = new StringBuilder();
            public bool bResponseOk;

            public void CallService()
            {
                try
                {
                    sReturn.Clear();
                    string url = "http://localhost:6152/historian/timeseriesdata/read/historic/" + sInputPMUs + "/" + dtInitDateTime.ToString("dd-MMM-yyyy HH:mm:ss.fff") + "/" + dtEndDateTime.ToString("dd-MMM-yyyy HH:mm:ss.fff") + "/json";
                    WebRequest request = WebRequest.Create(url);
                    request.Credentials = CredentialCache.DefaultCredentials;
                    request.Timeout = iTimeoutInSecs * 1000;
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    Stream dataStream = response.GetResponseStream();
                    StreamReader reader = new StreamReader(dataStream);
                    sReturn.Append(reader.ReadToEnd());

                    sReturn.Replace("{\"TimeSeriesDataPoints\":[", "");
                    sReturn.Replace("]}", "");
                    sReturn.Replace("},{", "}" + Environment.NewLine + "{");
                    bResponseOk = true;

                    reader.Close();
                    dataStream.Close();
                    response.Close();
                }
                catch (Exception e)
                {
                    sReturn.Clear();
                    sReturn.Append(e.Message);
                    bResponseOk = false;
                }
            }
        }

        private class AppendOnFile
        {
            public string file;
            public StringBuilder sTextToAppend = new StringBuilder();

            public void Append()
            {
                StreamWriter localOutputFile;
                localOutputFile = new System.IO.StreamWriter(file, true);
                localOutputFile.WriteLine(sTextToAppend);
                localOutputFile.Close();
            }
        }

        private static void bzCompact2Files(string sourceName, string targetName)
        {
            ProcessStartInfo p = new ProcessStartInfo();
            p.FileName = "7za.exe";
            p.Arguments = "a -tbzip2 \"" + targetName + "\" \"" + sourceName + "\" -mx=9";
            p.WindowStyle = ProcessWindowStyle.Hidden;
            Process x = Process.Start(p);
            x.WaitForExit();
        }

        static void Main(string[] args)
        {
            string dirName = ConfigurationManager.AppSettings["LocalizacaoArchive"];
            // dirName = "D:\\openPDC\\ArchiveTemp";
            string name = "";
            bool found = false;
            List<string> PMUNames = new List<string>();
            List<int> freqHistorianId = new List<int>();
            Dictionary<int, string> PMUsDictionary = new Dictionary<int, string>();
            string[] fileEntries = Directory.GetFiles(dirName);
            int iNbOfCalls = 0;
            DateTime now;
            AppendOnFile appendOnLog = new AppendOnFile();
            string sLocalOutputFileName = "";
            StringBuilder sLogText = new StringBuilder();
            TransferUtility transfer = new TransferUtility(new AmazonS3Client(Amazon.RegionEndpoint.USEast1));

            transfer.Download("log.dat", "pmu-data", "log.dat");

            foreach (string fileName in fileEntries)
            {
                if (fileName.IndexOf("archive.d") != -1)
                {
                    name = fileName;
                    found = true;
                }
            }

            if (found)
            {
                m_archiveFiles = new List<ArchiveFile>();
                m_metadata = new List<MetadataWrapper>();
                OpenArchive(name);

                foreach (ArchiveFile fileName in m_archiveFiles)
                {
                    foreach (MetadataRecord record in fileName.MetadataFile.Read())
                    {

                        if (record.GeneralFlags.Enabled && record.Description.IndexOf("Frequency") != -1 && record.Description.IndexOf("Delta") == -1)
                        {
                            PMUsDictionary.Add(record.HistorianID, record.SystemName);
                        }
                    }
                }

                // ---------------------------------------- Deleting service log file -------------------------------------------------
                // Because this file must be recreated at each execution
                if (File.Exists("service.log"))
                    File.Delete("service.log");
                // -------------------------------------------------------------------------------------------------------------------

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# PMUs recuperados - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                sLogText.Clear();
                // -------------------------------------------------------------------------------------------------------------------

                // Due to the instability of the openPDC service that will be called, and to the huge
                // ammount of data returned by it, the PMUs will be divided into subsets, and for each of
                // these subsets the service will be called a number of times - in intervals whose
                // duration is calculated as a function of the overall horizon and the duration in hours.

                // The following lines configure the execution based on the data in the appconfig: its start time, its overal
                // duration, the number of PMUs in each subset of PMUs and the duration of each service call.

                pdcServiceCaller oCallerObj = new pdcServiceCaller();
                AppendOnFile appendOnFile = new AppendOnFile();
                int iPMUSubsetIndex = 0;
                int iIntervalIndex = 0;
                string sCurrPMUParam = "";
                string sStartTime;
                int iStartHour;
                int iStartMinute;
                int iStartSecond;
                string sStartDaylightSaving;
                string sEndDaylightSaving;
                int iSizeOfPMUsSubset;
                DateTime dtStartDaylightSaving;
                DateTime dtEndDaylightSaving;
                DateTime dtToday = DateTime.Today;
                DateTime dtDateTimeStart = new DateTime(2000, 1, 31);
                DateTime dtDateTimeEnd;
                int iTimeoutInSecs;
                int iLimitOfAttempts;
                int iNbOfAttempts = 0;
                int iServiceCallHorizonInHours;
                int iTotalHorizonInHours;
                int iStartDay = 1;
                int iStartMonth = 1;
                int iStartYear = 2000;

                iSizeOfPMUsSubset = Convert.ToInt16(ConfigurationManager.AppSettings["TamanhoSubconjuntoPMUs"]);
                sStartTime = ConfigurationManager.AppSettings["HoraInicio"];
                if (ConfigurationManager.AppSettings["Repeticao"] == "UmaVez")
                {
                    iStartHour = Convert.ToInt16(sStartTime.Substring(0, 2));
                    iStartMinute = Convert.ToInt16(sStartTime.Substring(3, 2));
                    iStartSecond = Convert.ToInt16(sStartTime.Substring(6, 2));
                }
                else
                {
                    iStartHour = 3; // 03:00 GMT = 00:00 GMT-3. A piece of code ahead treats daylight saving time.
                    iStartMinute = 0;
                    iStartSecond = 0;
                }

                sStartDaylightSaving = ConfigurationManager.AppSettings["InicioHorarioDeVerao"];
                sEndDaylightSaving = ConfigurationManager.AppSettings["FimHorarioDeVerao"];
                dtStartDaylightSaving = new DateTime(Convert.ToInt16(sStartDaylightSaving.Substring(6, 4)),
                                                     Convert.ToInt16(sStartDaylightSaving.Substring(3, 2)),
                                                     Convert.ToInt16(sStartDaylightSaving.Substring(0, 2)));
                dtEndDaylightSaving = new DateTime(Convert.ToInt16(sEndDaylightSaving.Substring(6, 4)),
                                                   Convert.ToInt16(sEndDaylightSaving.Substring(3, 2)),
                                                   Convert.ToInt16(sEndDaylightSaving.Substring(0, 2)));

                if (dtToday >= dtStartDaylightSaving && dtToday <= dtEndDaylightSaving)
                    iStartHour--;

                // ---------------------- REAL CODE -------------------------------
                if (ConfigurationManager.AppSettings["Repeticao"] == "Diaria")
                {
                    dtDateTimeStart = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day, iStartHour, iStartMinute, iStartSecond);
                    dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                }
                else
                    if ((ConfigurationManager.AppSettings["Repeticao"] == "UmaVez"))
                    {
                        iStartDay = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(0, 2));
                        iStartMonth = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(3, 2));
                        iStartYear = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(6, 4));
                        dtDateTimeStart = new DateTime(iStartYear, iStartMonth, iStartDay, iStartHour, iStartMinute, iStartSecond);
                    }
                sLocalOutputFileName = dtDateTimeStart.ToString("yyyyMMdd") + ".json";
                // ----------------------------------------------------------------

                // --------------------- DEBUG CODE -------------------------------
                // dtDateTimeStart = new DateTime(2015, 1, 19, 2, 30, 00);
                // dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                // ----------------------------------------------------------------

                iTimeoutInSecs = Convert.ToInt16(ConfigurationManager.AppSettings["TimeoutDoServicoEmSegs"]);
                oCallerObj.iTimeoutInSecs = iTimeoutInSecs;
                
                iLimitOfAttempts = Convert.ToInt16(ConfigurationManager.AppSettings["NumDeTentativasDoServico"]);

                // IMPORTANT: the possibility of iTotalHorizonInHours / iServiceCallHorizonInHours, calculated below,
                // not being an integer is NOT COVERED in the code.
                iTotalHorizonInHours = Convert.ToInt16(ConfigurationManager.AppSettings["IntervaloTotalEmHoras"]);
                iServiceCallHorizonInHours = Convert.ToInt16(ConfigurationManager.AppSettings["IntervaloDoServicoEmHoras"]);
                iNbOfCalls = iTotalHorizonInHours / iServiceCallHorizonInHours;

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# Início das chamadas do serviço - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                sLogText.Clear();
                // -------------------------------------------------------------------------------------------------------------------

                appendOnFile.file = sLocalOutputFileName;

                foreach (var pair in PMUsDictionary)
                {
                    if (iPMUSubsetIndex < iSizeOfPMUsSubset)
                    {
                        if (iPMUSubsetIndex == 0)
                            sCurrPMUParam = Convert.ToString(pair.Key);
                        else
                            sCurrPMUParam = sCurrPMUParam + "," + pair.Key;
                        iPMUSubsetIndex++;
                    }
                    else
                    {
                        // ----------------------------------------------- REAL CODE -------------------------------------------------
                        if (ConfigurationManager.AppSettings["Repeticao"] == "Diaria")
                        {
                            dtDateTimeStart = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day, iStartHour, iStartMinute, iStartSecond);
                            dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                        }
                        else
                            if ((ConfigurationManager.AppSettings["Repeticao"] == "UmaVez"))
                            {
                                iStartDay = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(0, 2));
                                iStartMonth = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(3, 2));
                                iStartYear = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(6, 4));
                                dtDateTimeStart = new DateTime(iStartYear, iStartMonth, iStartDay, iStartHour, iStartMinute, iStartSecond);
                            }
                        dtDateTimeEnd = dtDateTimeStart.AddHours(iServiceCallHorizonInHours).AddMilliseconds(-1);
                        // -----------------------------------------------------------------------------------------------------------

                        // --------------------- DEBUG CODE -------------------------------
                        // dtDateTimeStart = new DateTime(2015, 1, 19, 2, 30, 00);
                        // dtDateTimeEnd = dtDateTimeStart.AddHours(4).AddMilliseconds(-1);
                        // ----------------------------------------------------------------

                        for (iIntervalIndex = 0; iIntervalIndex < iNbOfCalls; iIntervalIndex++) // Creates the intervals for each subset
                        {
                            // Filling the parameters
                            // oCallerObj = new pdcServiceCaller();
                            oCallerObj.sInputPMUs = sCurrPMUParam;
                            oCallerObj.dtInitDateTime = dtDateTimeStart;
                            oCallerObj.dtEndDateTime = dtDateTimeEnd;
                            oCallerObj.bResponseOk = false;
                            iNbOfAttempts = 0;
                            while(!oCallerObj.bResponseOk&&iNbOfAttempts<iLimitOfAttempts)
                            {
                                now = DateTime.Now;
                                appendOnLog.file = "service.log";
                                sLogText.Append(Convert.ToString(now) + ": PMUs: " + sCurrPMUParam + " - InitDateTime: " + Convert.ToString(dtDateTimeStart) + " - EndDateTime: " + Convert.ToString(dtDateTimeEnd) + " - Attempt: " + Convert.ToString(iNbOfAttempts));
                                appendOnLog.sTextToAppend = sLogText;
                                appendOnLog.Append();
                                sLogText.Clear();
                                Console.WriteLine(Convert.ToString(now) + ": PMUs: " + sCurrPMUParam + " - InitDateTime: " + Convert.ToString(dtDateTimeStart) + " - EndDateTime: " + Convert.ToString(dtDateTimeEnd) + " - Attempt: " + Convert.ToString(iNbOfAttempts));
                                oCallerObj.CallService();
                                iNbOfAttempts++;
                            }
                            if(iNbOfAttempts==iLimitOfAttempts&&!oCallerObj.bResponseOk)
                            {
                                // ------------------------------------------ Writing to log file ---------------------------------------------------
                                appendOnLog.file = "log.dat";
                                sLogText.Append(Environment.NewLine + "# Dados não gerados para " + sCurrPMUParam + " " + dtDateTimeStart.ToString("dd/MM/yyyy HH:mm") + " - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                                sLogText.Append(Environment.NewLine + "# *** Exception *** -> " + oCallerObj.sReturn + " - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                                appendOnLog.sTextToAppend = sLogText;
                                appendOnLog.Append();
                                sLogText.Clear();
                                // ------------------------------------------------------------------------------------------------------------------
                            }
                            else
                            {
                                if (oCallerObj.sReturn.Length > 0)
                                {
                                    appendOnFile.sTextToAppend = oCallerObj.sReturn;
                                    appendOnFile.Append();
                                }
                                else
                                {
                                    // ------------------------------------------ Writing to log file ---------------------------------------------------
                                    appendOnLog.file = "log.dat";
                                    sLogText.Append(Environment.NewLine + "# Nenhum dado retornado para " + sCurrPMUParam + " - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                                    appendOnLog.sTextToAppend = sLogText;
                                    appendOnLog.Append();
                                    sLogText.Clear();
                                    // ------------------------------------------------------------------------------------------------------------------
                                }
                            }

                            //------------------ REAL CODE ---------------------
                            dtDateTimeStart = dtDateTimeStart.AddHours(iServiceCallHorizonInHours);
                            dtDateTimeEnd = dtDateTimeEnd.AddHours(iServiceCallHorizonInHours);
                            //--------------------------------------------------

                            //---------------- DEBUG CODE -------------------
                            // dtDateTimeStart = dtDateTimeStart.AddHours(4);
                            // dtDateTimeEnd = dtDateTimeEnd.AddHours(4);
                            //-----------------------------------------------
                        }
                        sCurrPMUParam = Convert.ToString(pair.Key);
                        iPMUSubsetIndex = 1;
                    }
                }

                if (iPMUSubsetIndex != 0)
                {
                    // ----------------------------------------------- REAL CODE -------------------------------------------------
                    if (ConfigurationManager.AppSettings["Repeticao"] == "Diaria")
                        dtDateTimeStart = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day, iStartHour, iStartMinute, iStartSecond);
                    else
                        if ((ConfigurationManager.AppSettings["Repeticao"] == "UmaVez"))
                        {
                            iStartDay = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(0, 2));
                            iStartMonth = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(3, 2));
                            iStartYear = Convert.ToInt16(ConfigurationManager.AppSettings["DataInicio"].Substring(6, 4));
                            dtDateTimeStart = new DateTime(iStartYear, iStartMonth, iStartDay, iStartHour, iStartMinute, iStartSecond);
                        }
                    dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                    dtDateTimeEnd = dtDateTimeStart.AddHours(iServiceCallHorizonInHours).AddMilliseconds(-1);
                    // -----------------------------------------------------------------------------------------------------------

                    // --------------------- DEBUG CODE -------------------------------
                    // dtDateTimeStart = new DateTime(2015, 1, 19, 2, 30, 00);
                    // dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                    // dtDateTimeEnd = dtDateTimeStart.AddHours(4).AddMilliseconds(-1);
                    // ----------------------------------------------------------------


                    for (iIntervalIndex = 0; iIntervalIndex < iNbOfCalls; iIntervalIndex++) // Creates the 6 intervals for each subset
                    {
                        // Filling the parameters
                        oCallerObj.sInputPMUs = sCurrPMUParam;
                        oCallerObj.dtInitDateTime = dtDateTimeStart;
                        oCallerObj.dtEndDateTime = dtDateTimeEnd;
                        iNbOfAttempts = 0;
                        while (!oCallerObj.bResponseOk && iNbOfAttempts < iLimitOfAttempts)
                        {
                            oCallerObj.CallService();
                            iNbOfAttempts++;
                        }
                        if (iNbOfAttempts == iLimitOfAttempts && !oCallerObj.bResponseOk)
                        {
                            // ------------------------------------------ Writing to log file ---------------------------------------------------
                            appendOnLog.file = "log.dat";
                            sLogText.Append(Environment.NewLine + "# Dados não gerados para " + sCurrPMUParam + " " + dtDateTimeStart.ToString("dd/MM/yyyy HH:mm") + " - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                            sLogText.Append(Environment.NewLine + "# *** Exception *** -> " + oCallerObj.sReturn + " - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                            appendOnLog.sTextToAppend = sLogText;
                            appendOnLog.Append();
                            sLogText.Clear();
                            // ------------------------------------------------------------------------------------------------------------------
                        }
                        else
                        {
                            if(oCallerObj.sReturn.Length>0)
                            {
                                appendOnFile.sTextToAppend = oCallerObj.sReturn;
                                appendOnFile.Append();
                            }
                        }

                        //------------------ REAL CODE ---------------------
                        dtDateTimeStart = dtDateTimeStart.AddHours(iServiceCallHorizonInHours);
                        dtDateTimeEnd = dtDateTimeEnd.AddHours(iServiceCallHorizonInHours);
                        //--------------------------------------------------

                        //---------------- DEBUG CODE -------------------
                        // dtDateTimeStart = dtDateTimeStart.AddHours(4);
                        // dtDateTimeEnd = dtDateTimeEnd.AddHours(4);
                        //-----------------------------------------------
                    }
                }

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# Arquivo de saída gerado com sucesso - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                sLogText.Clear();
                // -------------------------------------------------------------------------------------------------------------------

                bzCompact2Files(sLocalOutputFileName, sLocalOutputFileName + ".bz2");

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# Arquivo de saída compactado - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                sLogText.Clear();
                // -------------------------------------------------------------------------------------------------------------------

                transfer.Upload(sLocalOutputFileName + ".bz2", "pmu-data");

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# Arquivo de saída carregado com sucesso - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                sLogText.Clear();
                // -------------------------------------------------------------------------------------------------------------------

                System.IO.File.Delete(sLocalOutputFileName);
                System.IO.File.Delete(sLocalOutputFileName + ".bz2");

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# Arquivos locais removidos - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                sLogText.Clear();
                // -------------------------------------------------------------------------------------------------------------------

                // ------------------------------------------ Writing to log file ----------------------------------------------------
                appendOnLog.file = "log.dat";
                sLogText.Append("# ================================================================");
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();
                // -------------------------------------------------------------------------------------------------------------------

                transfer.Upload("log.dat", "pmu-data");
                System.IO.File.Delete("log.dat");

                ClearArchives();
            }
        }

        private static void OpenArchives(string[] fileNames)
        {
            ClearArchives();
            // Console.WriteLine("Inside OpenArchives function...");
            foreach (string fileName in fileNames)
            {
                if (File.Exists(fileName))
                    m_archiveFiles.Add(OpenArchiveFile(fileName));
            }

        }

        private static void OpenArchive(string fileName)
        {
            OpenArchives(new string[] { fileName });
        }

        private static ArchiveFile OpenArchiveFile(string fileName)
        {

            string m_archiveLocation = FilePath.GetDirectoryName(fileName);
            string instance = fileName.Substring(0, fileName.LastIndexOf('_'));
            ArchiveFile file = new ArchiveFile();
            file.FileName = fileName;
            file.FileAccessMode = FileAccess.Read;

            file.StateFile = new StateFile();
            file.StateFile.FileAccessMode = FileAccess.Read;
            file.StateFile.FileName = string.Format("{0}_startup.dat", instance);

            file.IntercomFile = new IntercomFile();
            file.IntercomFile.FileAccessMode = FileAccess.Read;
            file.IntercomFile.FileName = string.Format("{0}scratch.dat", m_archiveLocation);

            file.MetadataFile = new MetadataFile();
            file.MetadataFile.FileAccessMode = FileAccess.Read;
            file.MetadataFile.FileName = string.Format("{0}_dbase.dat", instance);
            file.MetadataFile.LoadOnOpen = true;

            file.Open();
            return file;
        }

        private static void ClearArchives()
        {
            foreach (ArchiveFile file in m_archiveFiles)
                file.Close();

            m_archiveFiles.Clear();
            m_metadata.Clear();
        }
    }
}