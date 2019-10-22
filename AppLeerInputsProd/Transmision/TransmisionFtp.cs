using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppLeerInputs.Transmision
{
    public class TransmisionFtp
    {
        public void DownloadFileSSHNET()
        {

            string host = "52.0.144.141";
            string username = "vendemas-geo";
            string password = "8-t>VBm=Bh*J";
            string localFileName = @"E:\testk\";
            string remoteDirectory = "/data/vendemas-geo/SFTP";
            


            using (var sftp = new SftpClient(host, 22, username, password))
            {
                sftp.Connect();

                var files = sftp.ListDirectory(remoteDirectory);
                foreach (var file in files)
                {
                    Console.WriteLine(file.FullName);
                    string nomfile = Path.GetFileName(file.FullName);

                    using (var fileD = File.OpenWrite(localFileName + nomfile))
                    {
                        sftp.DownloadFile(file.FullName, fileD);
                    }
                }

                sftp.Disconnect();
            }
        }
    }
}
