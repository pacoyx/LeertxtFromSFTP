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

        string host_pmc = "";
        int puerto_pmc = 22;
        string username_pmc = "";
        string password_pmc = "";
        string carpetaLocal_pmc = "";
        string carpetaRemota_pmc = "";



        Logger loggerx = LogManager.GetCurrentClassLogger();

        private void LeerConfiguracion_Origen_Input(int idproceso)
        {
            //geopagos
            this.host = ConfigurationManager.AppSettings["host_geo"].ToString();
            this.username = ConfigurationManager.AppSettings["username_geo"].ToString();
            this.puerto = int.Parse(ConfigurationManager.AppSettings["puerto_geo"]);
            this.password = ConfigurationManager.AppSettings["password_geo"].ToString();
            this.carpetaLocal = ConfigurationManager.AppSettings["carpetaLocal_geo"].ToString();
            this.carpetaRemota = ConfigurationManager.AppSettings["carpetaRemota_geo"].ToString();
            loggerx.Info(idproceso + "|Carga de parametros para conexion a ftp geopagos completado.");

            //pmc
            this.host_pmc = ConfigurationManager.AppSettings["host_pmc"].ToString();
            this.username_pmc = ConfigurationManager.AppSettings["username_pmc"].ToString();
            this.puerto_pmc = int.Parse(ConfigurationManager.AppSettings["puerto_pmc"]);
            this.password_pmc = ConfigurationManager.AppSettings["password_pmc"].ToString();
            this.carpetaLocal_pmc = ConfigurationManager.AppSettings["carpetaLocal_pmc"].ToString();
            this.carpetaRemota_pmc = ConfigurationManager.AppSettings["carpetaRemota_pmc"].ToString();
            loggerx.Info(idproceso + "|Carga de parametros para conexion a ftp pmc completado.");


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


            if (this.host_pmc == string.Empty ||
               this.username_pmc == string.Empty ||
               this.puerto_pmc.ToString() == string.Empty ||
               this.password_pmc == string.Empty ||
               this.carpetaLocal_pmc == string.Empty ||
               this.carpetaRemota_pmc == string.Empty
               )
            {
                loggerx.Warn(idproceso + "|Carga de parametros para conexion a ftp incompleto.");
            }





        }

        public void Download_InputGeoPagos(int idproceso, string filtroComercio, string filtroTrx, string carpeta)
        {
            try
            {

                this.LeerConfiguracion_Origen_Input(idproceso);

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

                    Console.WriteLine("comenzando proceso de descarga...");
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

        public void Download_InputPMC(int idproceso, string carpeta)
        {
            try
            {
                this.LeerConfiguracion_Origen_Input(idproceso);

                using (var sftp = new SftpClient(host, puerto, username, password))
                {
                    Console.WriteLine("conectando al sftp pmc....");

                    sftp.Connect();

                    loggerx.Info(idproceso + "|Conexion al sftp pmc correcto");
                    Console.WriteLine("conexion establecida correctamente...");                    
                    Console.WriteLine("comenzando proceso de descarga...");
                    Console.WriteLine("fecha/hora inicio: {0}", DateTime.UtcNow);

                    Directory.CreateDirectory(carpetaLocal_pmc + carpeta);
                    loggerx.Info(idproceso + "|Creacion de carpeta con la fecha de proceso: " + carpeta + " correcto");
                          
                    using (var fileD = File.OpenWrite(carpetaLocal_pmc + carpeta + @"\mc_009018443.csv"))
                    {
                        sftp.DownloadFile(carpetaRemota_pmc, fileD);
                    }
                   
                    sftp.Disconnect();
                }
            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio un error en la descarga de archivos de geopagos del sftp");
            }

            loggerx.Info(idproceso + "|Termino proceso de descarga de archivos del ftp de geopagos");
            Console.WriteLine("fecha/hora termino: {0}", DateTime.UtcNow);
            Console.WriteLine("termino proceso...");
            Console.WriteLine("desconectado...");

        }

        public void Download_Input_NBO(int idproceso, string carpeta) { }
    }

}



