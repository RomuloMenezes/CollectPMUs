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
            // private string responseFromServer = "";
            // public string sReturn = "";
            private StringBuilder responseFromServer = new StringBuilder();
            public StringBuilder sReturn = new StringBuilder();

            public void CallService()
            {
                responseFromServer.Clear();

                string url = "http://localhost:6152/historian/timeseriesdata/read/historic/" + sInputPMUs + "/" + dtInitDateTime.ToString("dd-MMM-yyyy HH:mm:ss.fff") + "/" + dtEndDateTime.ToString("dd-MMM-yyyy HH:mm:ss.fff") + "/json";

                WebRequest request = WebRequest.Create(url);
                request.Credentials = CredentialCache.DefaultCredentials;
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream dataStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(dataStream);
                responseFromServer.Append(reader.ReadToEnd());

                sReturn = responseFromServer.Replace("{\"TimeSeriesDataPoints\":[", "");
                sReturn = sReturn.Replace("]}", "");
                sReturn = sReturn.Replace("},{", "}" + Environment.NewLine + "{");

                reader.Close();
                dataStream.Close();
                response.Close();
            }
        }

        private class AppendOnFile
        {
            public StreamWriter file;
            public StringBuilder sTextToAppend = new StringBuilder();

            public void Append()
            {
                file.WriteLine(sTextToAppend);
            }
        }

        static void Main(string[] args)
        {
            string dirName = "D:\\openPDC\\Archive\\";
            string name = "";
            bool found = false;
            List<string> PMUNames = new List<string>();
            List<int> freqHistorianId = new List<int>();
            Dictionary<int, string> PMUsDictionary = new Dictionary<int, string>();
            string[] fileEntries = Directory.GetFiles(dirName);
            int iNbOfPMUsSubsets = 0;
            int iNbOfCalls = 0;
            StreamWriter localOutputFile;
            StreamWriter localLog;
            AppendOnFile appendOnLog = new AppendOnFile();
            string sLocalOutputFileName = "";
            StringBuilder sLogText = new StringBuilder();
            TransferUtility transfer = new TransferUtility(new AmazonS3Client(Amazon.RegionEndpoint.USEast1));

            transfer.Download("log.dat", "pmu-data", "log.dat");
            localLog = new System.IO.StreamWriter("log.dat", true, System.Text.Encoding.UTF8);
            appendOnLog.file = localLog;

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

                sLogText.Append(Environment.NewLine + "# PMUs recuperados - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));

                // Due to the instability of the openPDC service that will be called, the PMUs will be
                // divided in subsets of 4, and for each of these subsets the service will be called
                // 6 times - in intervals of 4 hours. Experiments have shown that the service remains
                // stable for these values. It tends to crash with more PMUs in each subset and with
                // intervals longer than 4 hours.
                // The lines below calculate the variables that will control the sevice calls.

                iNbOfPMUsSubsets = (PMUsDictionary.Count) / 4;
                if (iNbOfPMUsSubsets * 4 < PMUsDictionary.Count)
                    iNbOfPMUsSubsets++; // In case PMUsDictionary.Count is not an exact multple of 4

                iNbOfCalls = iNbOfPMUsSubsets * 6; // For each subset the service will be called 6 times
                // (intervals of 4 hours)

                sLogText.Append(Environment.NewLine + "# Número de chamadas calculado - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));

                pdcServiceCaller oCallerObj = new pdcServiceCaller();
                AppendOnFile appendOnFile = new AppendOnFile();
                int iPMUSubsetIndex = 0;
                int iIntervalIndex = 0;
                string sCurrPMUParam = "";
                string sDailyRepetitionTime;
                int iRepetitionHour;
                int iRepetitionMinute;
                int iIndexOfSemicolon;
                string sStartDaylightSaving;
                string sEndDaylightSaving;
                int iSizeOfPMUsSubset = 4;
                DateTime dtStartDaylightSaving;
                DateTime dtEndDaylightSaving;
                DateTime dtToday = DateTime.Today;
                DateTime dtDateTimeStart = new DateTime(2000, 1, 31);
                DateTime dtDateTimeEnd;

                // iSizeOfPMUsSubset = Convert.ToInt16(ConfigurationManager.AppSettings["TamanhoSubconjuntoPMUs"]);
                sDailyRepetitionTime = ConfigurationManager.AppSettings["HoraRepeticaoDiaria"];
                iIndexOfSemicolon = sDailyRepetitionTime.IndexOf(':');
                if (iIndexOfSemicolon == -1)
                {
                    iRepetitionHour = Convert.ToInt16(sDailyRepetitionTime);
                    iRepetitionMinute = 0;
                }
                else
                {
                    iRepetitionHour = Convert.ToInt16(sDailyRepetitionTime.Substring(0, 2));
                    iRepetitionMinute = Convert.ToInt16(sDailyRepetitionTime.Substring(3, 2));
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
                    iRepetitionHour--;

                sLogText.Append(Environment.NewLine + "# Início das chamadas do serviço - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));

                // --------------------- DEBUG CODE -------------------------------
                // dtDateTimeStart = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day, iRepetitionHour, iRepetitionMinute, 00);
                dtDateTimeStart = new DateTime(2015, 1, 19, 2, 30, 00);
                dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                // ----------------------------------------------------------------

                sLocalOutputFileName = dtDateTimeStart.ToString("yyyyMMdd") + ".json";
                localOutputFile = new System.IO.StreamWriter(sLocalOutputFileName, true);
                appendOnFile.file = localOutputFile;

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
                        // dtDateTimeStart = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day, iRepetitionHour, iRepetitionMinute, 00);
                        // dtDateTimeStart = new DateTime(2015, 1, 19, 3, 00, 00);
                        // dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                        // dtDateTimeEnd = dtDateTimeStart.AddHours(4).AddMilliseconds(-1);
                        // -----------------------------------------------------------------------------------------------------------

                        // --------------------- DEBUG CODE -------------------------------
                        dtDateTimeStart = new DateTime(2015, 1, 19, 2, 30, 00);
                        dtDateTimeEnd = dtDateTimeStart.AddHours(4).AddMilliseconds(-1);
                        // ----------------------------------------------------------------

                        for (iIntervalIndex = 0; iIntervalIndex < 6; iIntervalIndex++) // Creates the 6 intervals for each subset
                        {
                            // Filling the parameters
                            // oCallerObj = new pdcServiceCaller();
                            oCallerObj.sInputPMUs = sCurrPMUParam;
                            oCallerObj.dtInitDateTime = dtDateTimeStart;
                            oCallerObj.dtEndDateTime = dtDateTimeEnd;
                            oCallerObj.CallService();

                            appendOnFile.sTextToAppend = oCallerObj.sReturn;
                            appendOnFile.Append();

                            //------------------ REAL CODE ---------------------
                            // dtDateTimeStart = dtDateTimeStart.AddHours(4);
                            // dtDateTimeEnd = dtDateTimeEnd.AddHours(4);
                            //--------------------------------------------------

                            //---------------- DEBUG CODE -------------------
                            dtDateTimeStart = dtDateTimeStart.AddHours(4);
                            dtDateTimeEnd = dtDateTimeEnd.AddHours(4);
                            //-----------------------------------------------
                        }
                        sCurrPMUParam = Convert.ToString(pair.Key);
                        iPMUSubsetIndex = 1;
                    }
                }

                if (iPMUSubsetIndex != 0)
                {
                    // ----------------------------------------------- REAL CODE -------------------------------------------------
                    // dtDateTimeStart = new DateTime(DateTime.Today.Year, DateTime.Today.Month, DateTime.Today.Day, iRepetitionHour, iRepetitionMinute, 00);
                    // dtDateTimeStart = new DateTime(2015, 1, 19, 3, 00, 00);
                    // dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                    // dtDateTimeEnd = dtDateTimeStart.AddHours(4).AddMilliseconds(-1);
                    // -----------------------------------------------------------------------------------------------------------

                    // --------------------- DEBUG CODE -------------------------------
                    dtDateTimeStart = new DateTime(2015, 1, 19, 2, 30, 00);
                    dtDateTimeStart = dtDateTimeStart.AddDays(-1);
                    dtDateTimeEnd = dtDateTimeStart.AddHours(4).AddMilliseconds(-1);
                    // ----------------------------------------------------------------


                    for (iIntervalIndex = 0; iIntervalIndex < 6; iIntervalIndex++) // Creates the 6 intervals for each subset
                    {
                        // Filling the parameters
                        // oCallerObj = new pdcServiceCaller();
                        oCallerObj.sInputPMUs = sCurrPMUParam;
                        oCallerObj.dtInitDateTime = dtDateTimeStart;
                        oCallerObj.dtEndDateTime = dtDateTimeEnd;
                        oCallerObj.CallService();

                        appendOnFile.sTextToAppend = oCallerObj.sReturn;
                        appendOnFile.Append();

                        //------------------ REAL CODE ---------------------
                        // dtDateTimeStart = dtDateTimeStart.AddHours(4);
                        // dtDateTimeEnd = dtDateTimeEnd.AddHours(4);
                        //--------------------------------------------------

                        //---------------- DEBUG CODE -------------------
                        dtDateTimeStart = dtDateTimeStart.AddHours(4);
                        dtDateTimeEnd = dtDateTimeEnd.AddHours(4);
                        //-----------------------------------------------
                    }
                }

                sLogText.Append(Environment.NewLine + "# Arquivo de saída gerado com sucesso - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));
                localOutputFile.Close();

                bzCompact2Files(sLocalOutputFileName, sLocalOutputFileName + ".bz2");
                sLogText.Append(Environment.NewLine + "# Arquivo de saída compactado - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));

                transfer.Upload(sLocalOutputFileName + ".bz2", "pmu-data");
                sLogText.Append(Environment.NewLine + "# Arquivo de saída carregado com sucesso - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));

                System.IO.File.Delete(sLocalOutputFileName);
                System.IO.File.Delete(sLocalOutputFileName + ".bz2");
                sLogText.Append(Environment.NewLine + "# Arquivos locais removidos - " + DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss"));

                sLogText.Append(Environment.NewLine + "# ================================================================");
                appendOnLog.sTextToAppend = sLogText;
                appendOnLog.Append();

                localLog.Close();
                transfer.Upload("log.dat", "pmu-data");

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

        private static void bzCompact2Files(string sourceName, string targetName)
        {
            ProcessStartInfo p = new ProcessStartInfo();
            p.FileName = "7za.exe";
            p.Arguments = "a -tbzip2 \"" + targetName + "\" \"" + sourceName + "\" -mx=9";
            p.WindowStyle = ProcessWindowStyle.Hidden;
            Process x = Process.Start(p);
            x.WaitForExit();
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