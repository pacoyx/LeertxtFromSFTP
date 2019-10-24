using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Renci.SshNet;
using System.IO;
using System.Configuration;
using System.Threading;
using NLog;

namespace AppLeerInputs.Transmision
{
    public class TransmisionFtp
    {

        string host = "";
        int puerto = 22;
        string username = "";
        string password = "";
        string carpetaLocal = "";
        string carpetaRemota = "";

        Logger loggerx = LogManager.GetCurrentClassLogger();

        private void LeerConfiguracion_Geo(int idproceso)
        {

            this.host = ConfigurationManager.AppSettings["host_geo"].ToString();
            this.username = ConfigurationManager.AppSettings["username_geo"].ToString();
            this.puerto = int.Parse(ConfigurationManager.AppSettings["puerto_geo"]);
            this.password = ConfigurationManager.AppSettings["password_geo"].ToString();
            this.carpetaLocal = ConfigurationManager.AppSettings["carpetaLocal_geo"].ToString();
            this.carpetaRemota = ConfigurationManager.AppSettings["carpetaRemota_geo"].ToString();

            loggerx.Info(idproceso + "|Carga de parametros para conexion a ftp completado.");

            if (this.host == string.Empty ||
                this.username == string.Empty ||
                this.puerto.ToString() == string.Empty ||
                this.password == string.Empty ||
                this.carpetaLocal == string.Empty ||
                this.carpetaRemota == string.Empty
                )
            {
                loggerx.Warn(idproceso + "|Carga de parametros para conexion a ftp incompleto.");
            }

        }

        public void Download_InputGeoPagos(int idproceso, string filtroComercio, string filtroTrx, string carpeta)
        {
            try
            {

                this.LeerConfiguracion_Geo(idproceso);

                using (var sftp = new SftpClient(host, puerto, username, password))
                {
                    Console.WriteLine("conectando al sftp....");

                    sftp.Connect();

                    loggerx.Info(idproceso + "|Conexion al sftp correcto");
                    Console.WriteLine("conexion establecida correctamente...");
                    Console.WriteLine("Cargando archivos remoto en memoria...");

                    var files = sftp.ListDirectory(carpetaRemota);
                    loggerx.Info(idproceso + "|Numero de archivos en memoria : " + files.Count<object>());
                    Console.WriteLine("Numero de archivos en memoria : {0}", files.Count<object>());

                    Console.WriteLine("comenzando proceso...");
                    Console.WriteLine("fecha/hora inicio: {0}", DateTime.UtcNow);

                    Directory.CreateDirectory(carpetaLocal + carpeta);
                    loggerx.Info(idproceso + "|Creacion de carpeta con la fecha de proceso: " + carpeta + " correcto");
                    int cont = 0;

                    var progress = new ProgressBar();

                    //foreach (var file in files)
                    Parallel.ForEach(files, (currentFile) =>
                    {
                        string nomfile = Path.GetFileName(currentFile.FullName);

                        if (nomfile != "." && nomfile != "..")
                        {
                            if (nomfile.Contains(filtroTrx))
                            {
                                using (var fileD = File.OpenWrite(carpetaLocal + carpeta + @"\" + filtroTrx + ".txt"))
                                {
                                    sftp.DownloadFile(currentFile.FullName, fileD);
                                }
                            }

                            if (nomfile.Contains(filtroComercio))
                            {
                                using (var fileD = File.OpenWrite(carpetaLocal + carpeta + @"\" + nomfile))
                                {
                                    sftp.DownloadFile(currentFile.FullName, fileD);
                                }
                            }
                        }

                        cont++;
                        progress.Report((double)cont / files.Count<object>());
                        //Thread.Sleep(20);                    
                    });

                    progress = null;
                    sftp.Disconnect();
                }
            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso+"|Ocurrio un error en la descarga de archivos de geopagos del sftp");
            }

            loggerx.Info(idproceso + "|Termino proceso de descarga de archivos del ftp de geopagos");
            Console.WriteLine("fecha/hora termino: {0}", DateTime.UtcNow);
            Console.WriteLine("termino proceso...");
            Console.WriteLine("desconectado...");

        }


    }

}



