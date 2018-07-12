using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

using System.Xml;
using System.IO;
using System.Configuration;

namespace MissingTagValue
{
    /// <summary>
    /// Look for DSD files with a missing ConversionDocID tag Value 
    /// //and cretes the corresponding Asset delete file to remove the record from Indexes
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //Create the XmlDocument.
                XmlDocument doc = new XmlDocument();
                int filesFound = 0;
                int currentDir = 0;
                int filesPerDirectoryCount = 0;
                int recordsCount = 0;

                string sourceDirectory = ConfigurationManager.AppSettings["SourceDirectory"];
                string targetDirectory = ConfigurationManager.AppSettings["TargetDirectory"];

                string[] files = System.IO.Directory.GetFiles(sourceDirectory, "*.dsd", System.IO.SearchOption.AllDirectories);

                foreach (string file in files)
                {
                    recordsCount = recordsCount + 1;
                    Console.WriteLine("Record #: " + recordsCount.ToString());
                    doc.Load(file);
                    using (XmlNodeList elemList = doc.GetElementsByTagName("UserField"))
                    {
                        for (int i = 0; i < elemList.Count; i++)
                        {
                            //Console.WriteLine(elemList[i].InnerXml);
                            if (elemList[i].InnerXml.Contains("<UserFieldTag>ConversionDocID</UserFieldTag><UserFieldValue />"))
                            {
                                //currentDir = filesFound % 1000;
                                //Console.WriteLine("Item Found ..");
                                Console.WriteLine(" File: " + file);
                                Console.WriteLine(" File name: " + Path.GetFileNameWithoutExtension(file));
                                filesFound = filesFound + 1;
                                filesPerDirectoryCount = filesPerDirectoryCount + 1;
                                if (filesPerDirectoryCount == 1000)
                                {
                                    filesPerDirectoryCount = 0;
                                    currentDir = currentDir + 1;
                                }
                                if (!System.IO.Directory.Exists(targetDirectory + "\\" + currentDir + "\\" + Path.GetFileNameWithoutExtension(file)))
                                {
                                    System.IO.Directory.CreateDirectory(targetDirectory + "\\" + +currentDir + "\\" + Path.GetFileNameWithoutExtension(file));
                                    //Copy dsd file 
                                    System.IO.File.Copy(file, targetDirectory + "\\" + currentDir + "\\" + Path.GetFileNameWithoutExtension(file) + "\\d@0@" + Path.GetFileName(file));                                   
                                }
                                break;
                            }
                        }
                    }

                }
                Console.WriteLine("Number of DSD files found: " + filesFound.ToString());
            }
            catch (IOException ex)
            {               
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(ex.ToString());
                    Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }
}
